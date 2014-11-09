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

import pandas as pd
import pandas.io.sql as psql

from scipy.stats import norm

from ema_config import *

global V

class AttrLogger(object):
	def __init__(self):
		super(AttrLogger, self).__setattr__('start_time', time.clock())
	def __setattr__(self, name, value):
		print "{0:6.1f} Finished calculating {1}.".format(time.clock()-self.start_time, name)
		super(AttrLogger, self).__setattr__(name, value)

V = AttrLogger()

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

def fetch_market_data(days=400, window=10, region=10000002, debug=global_debug):
	global V
	print 'Fetching market data ...'
	raw_query = \
		'''SELECT itemid, price_date, volume, avgprice
		   FROM crest_markethistory
		   WHERE regionid = %s
		   AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY
		   ORDER BY itemid, price_date''' % (region, days)
	if debug:
		raw_query = \
		'''SELECT itemid, price_date, volume, avgprice
		   FROM crest_markethistory
		   WHERE regionid = %s
		   AND itemid = 34
		   AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL %s DAY
		   ORDER BY itemid, price_date''' % (region, days)

	V.raw_query = raw_query
	raw_data = psql.read_sql(raw_query, data_conn, parse_dates=['price_date'])
	V.raw_data = raw_data
	expected_dates = pd.DataFrame(raw_data[raw_data.itemid == 34].price_date)
	expected_dates.index = expected_dates.price_date
	V.expected_dates = expected_dates
	raw_data_filled = pd.ordered_merge(
		raw_data[raw_data.itemid.isin(convert.index)], 
		expected_dates,
		on='price_date',
		left_by='itemid'
		)
	raw_data_filled.fillna({'volume':0}, inplace=True)
	raw_data_filled['price_delta_sma'] = \
		raw_data_filled \
		.groupby('itemid') \
		.avgprice \
		.apply(
			lambda x: x - pd.rolling_mean(
				x.interpolate()
				 .fillna(method='bfill'),
				window
				)
			)
	raw_data_filled['price_delta_smm'] = \
		raw_data_filled \
		.groupby('itemid') \
		.avgprice \
		.apply(
			lambda x: x - pd.rolling_median(
				x.interpolate()
				 .fillna(method='bfill'),
				window
				)
			)

	V.raw_data_filled = raw_data_filled
	return raw_data_filled.groupby('itemid')
	
def market_report(data_groups, report_sigmas, region=10000002, debug=global_debug):
	global V
	# only does volume atm.
	print 'Crunching Stats'
	print_array = []
	header = []
	header = build_header(report_sigmas)
	#if debug: print header
	print_array.append(header)
	
	def p(x, s): 
		c = x.dropna()
		ct = c.count()
		pctile = norm.cdf(-abs(s))
		return numpy.percentile(c, norm.cdf(s)*100) if ct >= 1/pctile else numpy.NaN

	def new_func(name, lam, l=locals()):
		lam.func_name = name
		l[name] = lam
		return lam

	N = new_func('N', lambda x: x.count())
	MIN = new_func('MIN', lambda x: x.min())
	P10 = new_func('P10', lambda x: numpy.percentile(x, 10))
	MED = new_func('MED', lambda x: numpy.median(x))
	AVG = new_func('AVG', lambda x: x.mean())
	P90 = new_func('P90', lambda x: numpy.percentile(x, 90))
	MAX = new_func('MAX', lambda x: x.max())
	STD = new_func('STD', lambda x: x.std())

	standard_stats = [N, MIN, P10, MED, AVG, P90, MAX, STD]
	sigma_stats = [
		new_func(sig_int_to_str(sigma), lambda x, s=sigma: p(x, s))
		for sigma in report_sigmas
		]
	stats = data_groups.agg(standard_stats + sigma_stats)
	V.stats = stats
	# slice the data and rename the columns so it's close to previous data.
	stats_vol = stats.loc[:,('volume','MIN'):('avgprice','N')]
	stats_vol.columns = stats_vol.columns.get_level_values(1)
	stats_final = pd.merge(convert, stats_vol, left_index=True, right_index=True)
	stats_final.index.name = 'typeid'
	stats_final.rename(columns={'name':'typename'}, inplace=True)
	cols = stats_final.columns.tolist()
	cols.insert(1,'N')
	cols.pop()
	stats_final = stats_final[cols]
	stats_final.to_csv('market_vol.csv')
	return stats_final

def sig_int_to_str(sigma_num):
	return "S{:.1f}".format(sigma_num).replace("-","N").replace(".","P")

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
	
def crunch_item_stats(item_id, data_group, expected_dates, report_sigmas, standard_stats = True):

	results_array = []
	data_array = vol_list
	n_count = len(vol_list)
	results_array.append(itemid)
	results_array.append(convert.at[itemid,'name'])
	results_array.append(n_count)
	
	filled_group = pd.mer

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
		
		pctile = norm.cdf(sigma)
			
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
	
def volume_sigma_report(
		market_sigmas, 
		data_groups, 
		filter_sigmas, 
		days, 
		vol_floor = 100, 
		region=10000002,
		debug=global_debug
		):
	global V
	# Drop all but last `days` data from all groups
	vol_means = data_groups.tail(10).groupby('itemid').mean()
	V.vol_means = vol_means
	return
	of_interest = pd.merge(vol_means, market_sigmas, left_on='itemid', right_on='itemid')
	V.of_interest = of_interest
	return
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
	for item in of_interest:
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
			item_name = convert.at[itemid,'name']
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

	convert = psql.read_sql(
		'''SELECT typeid as itemid, typename as name
 		   FROM invtypes conv
 		   JOIN invgroups grp ON (conv.groupID = grp.groupID)
 		   WHERE marketgroupid IS NOT NULL
 		   AND conv.published = 1
 		   AND grp.categoryid NOT IN (9,16,350001,2)
		   AND grp.groupid NOT IN (30,659,485,485,873,883)
 		   ORDER BY itemid''', 
		sde_conn, 
		index_col=['itemid']
		)
	V.convert = convert
	market_data_groups = fetch_market_data(region=region)
	V.market_data_groups = market_data_groups
	market_sigmas = market_report(market_data_groups, report_sigmas, region=region)
	V.market_sigmas = market_sigmas
	flaged_items_vol = volume_sigma_report(
		market_sigmas, 
		market_data_groups, 
		filter_sigmas, 
		15, 
		region=region
		)
	return ####
	#print flaged_items
	outfile = open('sig_flags.txt','w')
	for sig_level,itemids in flaged_items_vol.iteritems():
		outfile.write('%s FLAGS\n' % sig_level)
		for item in itemids:
			itemname=''
			try:
				itemname = convert.at[item,'name']
			except KeyError as e:
				pass
			outfile.write('\t%s,%s\n' % (item,itemname))
		
	outfile.close()
	
	print 'Plotting Flagged Group'
	fetch_and_plot(flaged_items_vol,region=region)
	
	R_config_file = open(conf.get('STATS','R_config_file'),'r')
	R_todo = json.load(R_config_file)
	fetch_and_plot(R_todo['forced_plots'],";addRSI();addLines(h=30, on=4);addLines(h=70, on=4)",region=region)
	print 'Plotting Forced Group'
	

def cuts_from_stats(stats, itemid, category):
	s = stats.loc[itemid, category]
	r = s['SN2P5':'S2P5']


if __name__ == "__main__":
	main()
else:
	main()
	
	#for region in trunc_region_list.iterkeys():
	#	print "Generating plots for {region}".format(region=region)
	#	main(region=region) 

# 30633 = wrecked weapon subroutines
# 12801 = Javelin M