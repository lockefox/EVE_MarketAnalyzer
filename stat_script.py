#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime
import numpy
import rpy2.robjects as robjects
import rpy2
from rpy2.robjects.packages import importr

from scipy.stats import norm

from ema_config import *

R_configured = False
img_type = conf.get('STATS','format')
img_X = conf.get('STATS','plot_width')
img_Y = conf.get('STATS','plot_height')

default_TA = conf.get('STATS','default_quantmod')
default_subset = conf.get('STATS','default_subset')

today = datetime.now().strftime('%Y-%m-%d')

convert = {}
global_debug = int(conf.get('STATS','debug'))

data_conn, data_cur, sde_conn, sde_cur = connect_local_databases()

def fetch_market_data_volume(days=366, region=10000002, debug=global_debug):
	#This needs to be merged into one fetch_market_data function
	print 'Fetching Volumes'
	if debug:
		data_cur.execute('''SELECT itemid,volume
						FROM crest_markethistory
						WHERE regionid = %s
						AND itemid = 34
						AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY
						ORDER BY price_date''' % (region,days))
	else:
		data_cur.execute('''SELECT itemid,volume
						FROM crest_markethistory
						WHERE regionid = %s
						AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY
						ORDER BY price_date''' % (region,days))
	raw_data = data_cur.fetchall()
	data_dict = {}
	for row in raw_data:
		if row[0] not in data_dict:
			data_dict[row[0]] = []
		data_dict[row[0]].append(row[1])
	
	return data_dict
	
def fetch_market_data(days=366, region=10000002, debug=global_debug):
	
	print 'Fetching Market Prices'
	if debug:
		data_cur.execute('''SELECT *
						FROM crest_markethistory
						WHERE regionid = %s
						AND itemid = 34
						AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY''' % (region,days))
	else:
		data_cur.execute('''SELECT itemid,volume
						FROM crest_markethistory
						WHERE regionid = %s
						AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY''' % (region,days))
	raw_data = data_cur.fetchall()
	data_dict = {}
	for row in raw_data:
		if row[0] not in data_dict:
			data_dict[row[0]] = []
		data_dict[row[0]].append(row[1])
	
	
	
def market_volume_report(data_dict, report_sigmas, region=10000002,debug=global_debug):
	print 'Crunching Stats'
	print_array = []
	header = []
	header = build_header(report_sigmas)
	#if debug: print header
	print_array.append(header)
	
	expected_length = len(data_dict[34])	#TRITANIUM expected to be complete for queried range
	for itemID,vol_array in data_dict.iteritems():
		data_row = []
		try:
			data_row.append(convert[itemID])
		except KeyError as e:
			print 'typeID %s not found' % itemID
			continue
		
		data_row = crunch_item_stats(itemID, vol_array, expected_length, report_sigmas)
		
		print_array.append(data_row)
	
	print 'Printing Results'
	outfile = open('market_vol.csv','w')
	for row in print_array:
		outstr = ''
		for col in row:
			outstr = '%s%s,' % (outstr,col)
		outstr = outstr[:-1]
		outfile.write('%s\n' % (outstr))
	outfile.close()	
	
	return_obj = dictify(print_array[0],print_array[1:])
	return return_obj

def sig_int_to_str(sigma_num):
	return "{:.1f}".format(sigma_num).replace("-","N").replace(".","P")

def build_header (report_sigmas,standard_stats = True):
	header = []
	header.append('typeid')
	header.append('typename')
	header.append('N')
	if standard_stats:
		header.append('MIN')
		header.append('P10')
		header.append('MED')
		header.append('AVG')
		header.append('P90')
		header.append('MAX')
		header.append('STD')
	for sigma_num in report_sigmas:
		sigma_str = sig_int_to_str(sigma_num)
		header.append(sigma_str)
		
	return header
	
def crunch_item_stats(itemid, vol_list, expected_length, report_sigmas, standard_stats = True):
	results_array = []
	data_array = vol_list
	n_count = len(vol_list)
	results_array.append(itemid)
	results_array.append(convert[itemid])
	results_array.append(n_count)
	
	if n_count < expected_length:	#append zeros to make sigmas match sample size
		for range in (0, expected_length - n_count):
			data_array.append(0)
			
	if standard_stats:
		results_array.append(numpy.amin(data_array))
		results_array.append(numpy.percentile(data_array,10))
		results_array.append(numpy.median(data_array))
		results_array.append(numpy.average(data_array))
		results_array.append(numpy.percentile(data_array,90))
		results_array.append(numpy.amax(data_array))
		results_array.append(numpy.std(data_array))
	
	for sigma in report_sigmas:
		
		pctile = norm.cdf(-abs(sigma))
			
		if n_count < (1/pctile):	#Not enough samples to report sigma value
			results_array.append(None)
		else:
			percent = pctile if sigma <= 0 else 1 - pctile
			results_array.append(numpy.percentile(data_array,percent*100))
	
	return results_array
	
def dictify(header_list,data_list):
	return_dict = {}
	for row in data_list:
		return_dict[row[0]] = {}
		header_index = 0
		for col in header_list:
			return_dict[row[0]][col] = row[header_index]
			header_index += 1
	
	return return_dict
	
def volume_sigma_report(market_sigmas, filter_sigmas, days, vol_floor = 100, region=10000002,debug=global_debug):
	print 'Fetching Short Volumes'
	
	if debug:
		data_cur.execute('''SELECT itemid,volume
						FROM crest_markethistory
						WHERE regionid = %s
						AND itemid = 34
						AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY''' %(region,days))
	else:
		data_cur.execute('''SELECT itemid,volume
						FROM crest_markethistory
						WHERE regionid = %s
						AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY''' %(region,days))
	raw_data = data_cur.fetchall()
	data_dict = {}
	for row in raw_data:
		if row[0] not in data_dict:
			data_dict[row[0]] = []
		data_dict[row[0]].append(row[1])
	
	#Build pseudo-header for report.  Top level keys for return dict
	result_dict = {}
	filter_sigmas.sort()
	if (0 in filter_sigmas) or (0.0 in filter_sigmas):
			print '0 Sigma (MED) not supported'
			#Not sure if > or < than MED for flagging.  Do MED flagging in another function
	for sigma in filter_sigmas:
		sig_str = sig_int_to_str(sigma)
		result_dict[sig_str] = []

	print 'Parsing data'
	for typeid,vol_list in data_dict.iteritems():
		flag_HIsigma = False	
		avg_value = numpy.average(vol_list)
		if avg_value < vol_floor:
			continue	#filter out very low volumes
		try:
			market_sigmas[typeid]
		except KeyError as e:
			continue
		
		filter_sigmas.sort()
		#check negative sigmas	
		for sigma in filter_sigmas:
			if sigma >=0:
				break	#do negative sigmas first
			sig_str = sig_int_to_str(sigma)
			flag_limit = market_sigmas[typeid][sig_str]
			if avg_value < flag_limit:
				result_dict[sig_str].append(typeid)
				break #most extreme sigma found, stop looking
		
		filter_sigmas.sort(reverse=True)
		#check positive sigmas
		for sigma in filter_sigmas:
			if sigma <=0:
				break	#don't do SIG0
			sig_str = sig_int_to_str(sigma)
			flag_limit = market_sigmas[typeid][sig_str]
			if avg_value > flag_limit:
				result_dict[sig_str].append(typeid)
				break #most extreme sigma found, stop looking
		
	return result_dict			

def fetch_and_plot(data_struct, TA_args = "", region=10000002):
	global R_configured
	if not R_configured:
		print 'Setting up R libraries'
		importr('jsonlite')
		importr('quantmod',robject_translations = {'skeleton.TA':'skeletonTA'})
		importr('data.table')
		R_configured = True

	print 'setting up dump path'
	if not os.path.exists('plots/%s' % today):
		os.makedirs('plots/%s' % today)
	
	for group, item_list in data_struct.iteritems():
		print 'crunching %s' % group
		dump_path = 'plots/%s/%s' % (today, group)
		if not os.path.exists(dump_path):
			os.makedirs(dump_path)
			
		for itemid in item_list:
			query_str = '%smarket/%s/types/%s/history/' % (crest_path, region, itemid)
			item_name = convert[itemid]
			if itemid == 29668:
				item_name = 'PLEX'	#Hard override, because PLEX name is dumb
			item_name = sanitize(item_name)	#remove special chars
			img_path = '{dump_path}/{item_name}_{region}_{today}.{img_type}'.format(
				dump_path=dump_path,
				region=region,
				item_name=item_name,
				today=today,
				img_type=img_type
				)
			plot_title = '%s %s' % (item_name, today)
			print '\tplotting %s' % item_name
			R_command_parametrized = '''
				market.json <- fromJSON(readLines('{query_str}'))
				market.data <- data.table(market.json$items)
				market.data <- market.data[,list(Date = as.Date(date),
												Volume= volume,
												High  = highPrice,
												Low   = lowPrice,
												Close =avgPrice[-1],
												Open  = avgPrice)]
				n <- nrow(market.data)
				market.data <- market.data[1:n-1,]
				market.data.ts <- xts(market.data[,-1,with=F],order.by=market.data[,Date],period=7)
				{img_type}('{img_path}',width = {img_X}, height = {img_Y})
				chartSeries(market.data.ts,
							name = '{plot_title}',
							TA = '{default_TA}{TA_args}',
							subset = '{default_subset}')
				dev.off()'''
			R_command = R_command_parametrized.format(
				query_str=query_str,
				img_type=img_type,
				img_path=img_path,
				img_X=img_X,
				img_Y=img_Y,
				plot_title=plot_title,
				default_TA=default_TA,
				TA_args=TA_args,
				default_subset=default_subset
				)


			#robjects.r(R_command)	
			#print R_command
			for tries in range (0,retry_limit):
				try:
					robjects.r(R_command)
				except rpy2.rinterface.RRuntimeError as e:
					print '\t\tFailed pull %s' % e
					continue
				break
			else:
				print '\t\tskipping %s' % item_name
				continue

def main(region=10000002):
	report_sigmas = [
		-2.5,
		-2.0,
		-1.5,
		-1.0,
		-0.5,
		 0.0,
		 0.5,
		 1.0,
		 1.5,
		 2.0,
		 2.5
	]
	
	filter_sigmas = [
		-2.5,
		-2.0,
		-1.5,
		#-1.0,
		 1.5,
		 2.0,
		 2.5
	]
	global convert
	print 'Fetching item list from SDE: %s' % sde_schema
	sde_cur.execute('''SELECT typeid,typename
						FROM invtypes conv
						JOIN invgroups grp ON (conv.groupID = grp.groupID)
						WHERE marketgroupid IS NOT NULL
						AND conv.published = 1
						AND grp.categoryid NOT IN (9,16,350001,2)
						AND grp.groupid NOT IN (30,659,485,485,873,883)''')
	tmp_convlist = sde_cur.fetchall()
	for row in tmp_convlist:
		convert[row[0]]=row[1]
	market_data_vol = fetch_market_data_volume(region=region)
	market_sigmas = market_volume_report(market_data_vol, report_sigmas, region=region)
	flaged_items_vol = volume_sigma_report(market_sigmas, filter_sigmas, 15, region=region)
	
	#print flaged_items
	outfile = open('sig_flags.txt','w')
	for sig_level,itemids in flaged_items_vol.iteritems():
		outfile.write('%s FLAGS\n' % sig_level)
		for item in itemids:
			itemname=''
			try:
				itemname = convert[item]
			except KeyError as e:
				None
			outfile.write('\t%s,%s\n' % (item,itemname))
		
	outfile.close()
	
	print 'Plotting Flagged Group'
	fetch_and_plot(flaged_items_vol,region=region)
	
	R_config_file = open(conf.get('STATS','R_config_file'),'r')
	R_todo = json.load(R_config_file)
	fetch_and_plot(R_todo['forced_plots'],";addRSI();addLines(h=30, on=4);addLines(h=70, on=4)",region=region)
	print 'Plotting Forced Group'
	

if __name__ == "__main__":
	for region in trunc_region_list.iterkeys():
		print "Generating plots for {region}".format(region=region)
		main(region=region) 
