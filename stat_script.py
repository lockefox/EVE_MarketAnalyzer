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

def market_volume_report(region=10000002,debug=False):
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
	header.append('typeid')
	header.append('typename')
	header.append('N')
	header.append('MIN')
	header.append('P10')
	header.append('MED')
	header.append('AVG')
	header.append('P90')
	header.append('MAX')
	header.append('STD')
	header.append('SIG_N2P5')
	header.append('SIG_N2P0')
	header.append('SIG_N1P5')
	header.append('SIG_N1P0')
	header.append('SIG_N0P5')
	header.append('SIG_0P0')
	header.append('SIG_0P5')
	header.append('SIG_1P0')
	header.append('SIG_1P5')
	header.append('SIG_2P0')
	header.append('SIG_2P5')
	print_array.append(header)
	
	for itemID,vol_array in data_dict.iteritems():
		data_row = []
		data_row.append(itemID)
		try:
			data_row.append(convert[itemID])
		except KeyError as e:
			print 'typeID %s not found' % itemID
			continue
		data_row.append(len(vol_array))
		data_row.append(numpy.amin(vol_array))
		
		entries_returned = len(vol_array)
		if len(vol_array) < 365:
			for range in (0, 364-len(vol_array)):
				vol_array.append(0)
		
		data_row.append(numpy.percentile(vol_array,10))
		data_row.append(numpy.median(vol_array))
		data_row.append(numpy.average(vol_array))
		data_row.append(numpy.percentile(vol_array,90))
		data_row.append(numpy.amax(vol_array))
		data_row.append(numpy.std(vol_array))
		
		##TODO: this is bad, and needs to be automated
		## IF(num > sig_threshold): save
		if entries_returned < SIG_2P5:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,0.62))
		
		if entries_returned < SIG_2P0:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,2.28))
		
		if entries_returned < SIG_1P5:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,6.68))
		
		if entries_returned < SIG_1P0:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,15.87))
		
		if entries_returned < SIG_0P5:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,30.85))
			
		data_row.append(numpy.percentile(vol_array,50))
		
		if entries_returned < SIG_0P5:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,69.15))	
			
		if entries_returned < SIG_1P0:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,84.13))
		
		if entries_returned < SIG_1P5:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,93.32))
		
		if entries_returned < SIG_2P0:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,97.72))
		
		if entries_returned < SIG_2P5:
			data_row.append(None)
		else:
			data_row.append(numpy.percentile(vol_array,99.38))
		
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
	
def dictify(header_list,data_list):
	return_dict = {}
	for row in data_list:
		return_dict[row[0]] = {}
		header_index = 0
		for col in header_list:
			return_dict[row[0]][col] = row[header_index]
			header_index += 1
	
	return return_dict
	
def sigma_report(market_sigmas, days, region=10000002,debug=False):
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
	result_dict['SIG_N1P5']=[]
	result_dict['SIG_N1P0']=[]
	#result_dict['SIG_N0P5']=[]	NO NEED TO FLAG MEDIAN SIGMAS
	#result_dict['SIG_0P0']=[]
	#result_dict['SIG_0P5']=[]
	result_dict['SIG_1P0']=[]
	result_dict['SIG_1P5']=[]
	result_dict['SIG_2P0']=[]
	result_dict['SIG_2P5']=[]
	
	for typeid,vol_list in data_dict.iteritems():
		flag_HIsigma = False
		flag_LOsigma = False
		#Flag "most extreme" sigma
		for value in vol_list:
			#This is bad.  Make it better
			try:
				market_sigmas[typeid]
			except KeyError as e:
				break
			if value > market_sigmas[typeid]['SIG_2P5']:
				if flag_HIsigma == False:
					result_dict['SIG_2P5'].append(typeid)
					flag_HIsigma = True
			if value > market_sigmas[typeid]['SIG_2P0']:
				if flag_HIsigma == False:
					result_dict['SIG_2P0'].append(typeid)
					flag_HIsigma = True
			if value > market_sigmas[typeid]['SIG_1P5']:
				if flag_HIsigma == False:
					result_dict['SIG_1P5'].append(typeid)
					flag_HIsigma = True
			if value > market_sigmas[typeid]['SIG_1P0']:
				if flag_HIsigma == False:
					result_dict['SIG_1P0'].append(typeid)
					flag_HIsigma = True
					
			if value < market_sigmas[typeid]['SIG_N2P5']:
				if flag_LOsigma == False:
					result_dict['SIG_N2P5'].append(typeid)
					flag_LOsigma = True
			if value < market_sigmas[typeid]['SIG_N2P0']:
				if flag_LOsigma == False:
					result_dict['SIG_N2P0'].append(typeid)
					flag_LOsigma = True
			if value < market_sigmas[typeid]['SIG_N1P5']:
				if flag_LOsigma == False:
					result_dict['SIG_N1P5'].append(typeid)
					flag_LOsigma = True
			if value < market_sigmas[typeid]['SIG_N1P0']:
				if flag_LOsigma == False:
					result_dict['SIG_N1P0'].append(typeid)
					flag_LOsigma = True
	return result_dict			

def main():
	market_sigmas = market_volume_report()
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