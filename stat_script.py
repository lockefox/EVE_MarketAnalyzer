#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime
import numpy

conf = ConfigParser.ConfigParser()
conf.read(['init.ini','init_local.ini'])
#####
#
#	3sig	= 0.13% (769)
#	2.5sig	= 0.62%	(161)
#	2sig	= 2.28%	(44)
#	1.5sig	= 6.68% (15)
#	1sig	= 15.87%(6)
#	0.5sig	= 30.85%(3)
#
#####

##TODO: dynamic limits
sigma_to_percentile = {
	-5  :2.86652e-7 ,
	-4.9:4.79183e-7 ,
	-4.8:7.93328e-7 ,
	-4.7:1.30081e-6 ,
	-4.6:2.11245e-6 ,
	-4.5:3.39767e-6 ,
	-4.4:5.41254e-6 ,
	-4.3:8.53991e-6 ,
	-4.2:1.33457e-5 ,
	-4.1:2.06575e-5 ,
	-4  :3.16712e-5 ,
	-3.9:4.80963e-5 ,
	-3.8:7.2348e-5,
	-3.7:0.0001078,
	-3.6:0.000159109,
	-3.5:0.000232629,
	-3.4:0.000336929,
	-3.3:0.000483424,
	-3.2:0.000687138,
	-3.1:0.000967603,
	-3  :0.001349898,
	-2.9:0.001865813,
	-2.8:0.00255513,
	-2.7:0.003466974,
	-2.6:0.004661188,
	-2.5:0.006209665,
	-2.4:0.008197536,
	-2.3:0.01072411 ,
	-2.2:0.013903448,
	-2.1:0.017864421,
	-2  :0.022750132,
	-1.9:0.02871656 ,
	-1.8:0.035930319,
	-1.7:0.044565463,
	-1.6:0.054799292,
	-1.5:0.066807201,
	-1.4:0.080756659,
	-1.3:0.096800485,
	-1.2:0.11506967 ,
	-1.1:0.135666061,
	-1  :0.158655254,
	-0.9:0.184060125,
	-0.8:0.211855399,
	-0.7:0.241963652,
	-0.6:0.274253118,
	-0.5:0.308537539,
	-0.4:0.344578258,
	-0.3:0.382088578,
	-0.2:0.420740291,
	-0.1:0.460172163,
	0   :0.5
}
SIG_3P0 = 769
SIG_2P5 = 161
SIG_2P0 = 44
SIG_1P5 = 15
SIG_1P0 = 6 
SIG_0P5 = 3

convert = {}
	
db_host   = conf.get('GLOBALS','db_host')
db_user   = conf.get('GLOBALS','db_user')
db_pw     = conf.get('GLOBALS','db_pw')
db_port   = int(conf.get('GLOBALS','db_port'))
db_schema = conf.get('GLOBALS','db_schema')
db_driver = conf.get('GLOBALS','db_driver')
sde_schema  = conf.get('GLOBALS','sde_schema')

data_conn = pypyodbc.connect('DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DATABASE=%s' \
	% (db_driver,db_host,db_port,db_user,db_pw,db_schema))
data_cur = data_conn.cursor()
sde_conn  = pypyodbc.connect('DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DATABASE=%s' \
	% (db_driver,db_host,db_port,db_user,db_pw,sde_schema))
sde_cur = sde_conn.cursor()

def market_volume_report(report_sigmas, region=10000002,debug=False):
	global convert
	
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
	
	print 'Fetching Volumes'
	if debug:
		data_cur.execute('''SELECT itemid,volume
						FROM crest_markethistory
						WHERE regionid = %s
						AND itemid = 34
						AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL 366 DAY''' %region)
	else:
		data_cur.execute('''SELECT itemid,volume
						FROM crest_markethistory
						WHERE regionid = %s
						AND price_date > (SELECT max(price_date) FROM crest_markethistory) - INTERVAL 366 DAY''' %region)
	raw_data = data_cur.fetchall()
	data_dict = {}
	for row in raw_data:
		if row[0] not in data_dict:
			data_dict[row[0]] = []
		data_dict[row[0]].append(row[1])
	
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
		sigma_str = 'SIG_'
		try:
			sigma_to_percentile[sigma_num]
		except KeyError as e:
			try:
				sigma_to_percentile[-sigma_num]
			except KeyError as e:
				print 'Sigma: %s not covered' % sigma_num
				sys.exit(2)
		
		(decimal, integer) = math.modf(sigma_num)
		if integer < 0:
			sigma_str = '%sN' % sigma_str
		
		sigma_str = '%s%sP%s' % (sigma_str,int(abs(integer)),int(abs(decimal*10))) #Like SIG_N2P5 or SIG_2P5	
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
		pos_sigma = False
		try:
			sigma_to_percentile[sigma]
		except KeyError as e:
			pos_sigma = True # lookup only covers negative sigmas.  some weirness required for positive ones
			try:
				sigma_to_percentile[-sigma]
			except KeyError as e:
				print 'Sigma: %s not covered' % sigma
				sys.exit(2)
			
		if n_count < (1/sigma_to_percentile[-abs(sigma)]):	#Not enough samples to report sigma value
			results_array.append(None)
		else:
			percent = 0.0
			if pos_sigma:
				percent = 1-(sigma_to_percentile[-sigma])
			else:
				percent = sigma_to_percentile[sigma]
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
	
def sigma_report(market_sigmas, days, vol_floor = 100, region=10000002,debug=False):
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
	#print data_dict
	#print market_sigmas

	result_dict = {}
	##TODO: make this dynamic
	result_dict['SIG_N2P5']=[]
	result_dict['SIG_N2P0']=[]
	#result_dict['SIG_N1P5']=[]
	#result_dict['SIG_N1P0']=[]
	#result_dict['SIG_N0P5']=[]	NO NEED TO FLAG MEDIAN SIGMAS
	#result_dict['SIG_0P0']=[]
	#result_dict['SIG_0P5']=[]
	#result_dict['SIG_1P0']=[]
	#result_dict['SIG_1P5']=[]
	result_dict['SIG_2P0']=[]
	result_dict['SIG_2P5']=[]
	
	for typeid,vol_list in data_dict.iteritems():
		flag_HIsigma = False
		flag_LOsigma = False
		#Flag "most extreme" sigma
		#for value in vol_list:
		value = numpy.average(vol_list)
		if value < vol_floor:
			continue
		#This is bad.  Make it better
		try:
			market_sigmas[typeid]
		except KeyError as e:
			continue
		if value > market_sigmas[typeid]['SIG_2P5']:
			if flag_HIsigma == False:
				result_dict['SIG_2P5'].append(typeid)
				flag_HIsigma = True
		if value > market_sigmas[typeid]['SIG_2P0']:
			if flag_HIsigma == False:
				result_dict['SIG_2P0'].append(typeid)
				flag_HIsigma = True
		#if value > market_sigmas[typeid]['SIG_1P5']:
		#	if flag_HIsigma == False:
		#		result_dict['SIG_1P5'].append(typeid)
		#		flag_HIsigma = True
		#if value > market_sigmas[typeid]['SIG_1P0']:
		#	if flag_HIsigma == False:
		#		result_dict['SIG_1P0'].append(typeid)
		#		flag_HIsigma = True
				
		if value < market_sigmas[typeid]['SIG_N2P5']:
			if flag_LOsigma == False:
				result_dict['SIG_N2P5'].append(typeid)
				flag_LOsigma = True
		if value < market_sigmas[typeid]['SIG_N2P0']:
			if flag_LOsigma == False:
				result_dict['SIG_N2P0'].append(typeid)
				flag_LOsigma = True
		#if value < market_sigmas[typeid]['SIG_N1P5']:
		#	if flag_LOsigma == False:
		#		result_dict['SIG_N1P5'].append(typeid)
		#		flag_LOsigma = True
		#if value < market_sigmas[typeid]['SIG_N1P0']:
		#	if flag_LOsigma == False:
		#		result_dict['SIG_N1P0'].append(typeid)
		#		flag_LOsigma = True
	return result_dict			

def main():
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
		 2.0,
		 2.5
	]
	
	market_sigmas = market_volume_report(report_sigmas)
	flaged_items = sigma_report(market_sigmas, 15)
	
	#print flaged_items
	outfile = open('sig_flags.txt','w')
	for sig_level,itemids in flaged_items.iteritems():
		outfile.write('%s FLAGS\n' % sig_level)
		for item in itemids:
			itemname=''
			try:
				itemname = convert[item]
			except KeyError as e:
				None
			outfile.write('\t%s,%s\n' % (item,itemname))
		
	outfile.close()
if __name__ == "__main__":
	main()