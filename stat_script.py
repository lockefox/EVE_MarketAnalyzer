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

def market_volume_report():
	sde_cur.execute('''SELECT typeid,typename
						FROM invtypes
						WHERE marketgroupid IS NOT NULL
						AND published = 1''')
	tmp_convlist = sde_cur.fetchall()
	convert = {}
	for row in tmp_convlist:
		convert[row[0]]=row[1]
	
	print 'Fetching Volumes'
	data_cur.execute('''SELECT itemid,volume
						FROM crest_markethistory
						WHERE regionid = 10000002
						AND price_date > NOW() - INTERVAL 366 DAY''')
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
		data_row.append(convert[itemID])
		data_row.append(len(vol_array))
		data_row.append(numpy.amin(vol_array))
		
		if len(vol_array) < 365:
			for range in (0, 364-len(vol_array)):
				vol_array.append(0)
		
		data_row.append(numpy.percentile(vol_array,10))
		data_row.append(numpy.median(vol_array))
		data_row.append(numpy.average(vol_array))
		data_row.append(numpy.percentile(vol_array,90))
		data_row.append(numpy.amax(vol_array))
		data_row.append(numpy.std(vol_array))
		data_row.append(numpy.percentile(vol_array,0.62))
		data_row.append(numpy.percentile(vol_array,2.28))
		data_row.append(numpy.percentile(vol_array,6.68))
		data_row.append(numpy.percentile(vol_array,15.87))
		data_row.append(numpy.percentile(vol_array,30.85))
		data_row.append(numpy.percentile(vol_array,50))
		data_row.append(numpy.percentile(vol_array,69.15))
		data_row.append(numpy.percentile(vol_array,84.13))
		data_row.append(numpy.percentile(vol_array,93.32))
		data_row.append(numpy.percentile(vol_array,97.72))
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
def main():
	market_volume_report()

if __name__ == "__main__":
	main()