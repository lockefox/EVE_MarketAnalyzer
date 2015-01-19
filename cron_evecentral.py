import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path, environ
import urllib2
import httplib
import requests
import urllib
import MySQLdb 
#ODBC connector not supported on pi/ARM platform
from datetime import datetime, timedelta
import threading

from ema_config import *
thread_exit_flag = False

db_con = None
db_cur = None

#### PROGRAM GLOBALS ####
itemlist = []
snapshot_table = conf.get('TABLES','cron_evecentral')
logfile_name = conf.get('CRON','evecentral_logfile') #add locationID to logfile name
evecentral_url = conf.get('CRON','evecentral_baseURL')
fetch_type = conf.get('CRON','evecentral_defaulttype')
table_header = ''	#will be an issue if _initSQL is called multiple times
start_datetime = datetime.utcnow()
commit_date = start_datetime.strftime('%Y-%m-%d')
commit_time = start_datetime.strftime('%H:%M:%S')

def thread_print(msg):
	sys.stdout.write("%s\n" % msg)
	sys.stdout.flush()
	
def query_locationType(locationID):
	#Returns supported query modifier.  Else blank to avoid bad calls
	digit = str(locationID)[:1]
	int_digit = int(digit)
	if int_digit == 1:
		return 'regionlimit'
	#elif  int_digit == 2:
	#	None #constellation not supported
	elif int_digit == 3:
		return 'usesystem'
	elif int_digit == 6:
		return 'usestation'
	else:
		return ''	#exception would be better

def fetch_typeIDs():
	global itemlist
	sde_con = MySQLdb.connect(
		host   = db_host,
		user   = db_user,
		passwd = db_pw,
		port   = db_port,
		db     = sde_schema)
	sde_cur = sde_con.cursor()

	query_filename = conf.get('CRON','evecentral_query')
	item_query = open(path.relpath('SQL/%s.mysql' % query_filename)).read()
	sde_cur.execute(item_query)
	raw_values = sde_cur.fetchall()
	
	return_list = []
	for row in raw_values:
		return_list.append(int(row[0]))
	return return_list
	
def writelog(locationID, message):
	None
	
def _initSQL(table_name):
	global db_con, db_cur, table_header
	
	db_con = MySQLdb.connect(
		host   = db_host,
		user   = db_user,
		passwd = db_pw,
		port   = db_port,
		db     = db_schema)
	db_cur = db_con.cursor()
	db_cur.execute('''SHOW TABLES LIKE \'%s\'''' % table_name)
	db_exists = len(db_cur.fetchall())
	if db_exists:
		print '%s.%s:\tGOOD' % (db_schema,table_name)
	else:	#TODO: add override command to avoid 'drop table' command 
		table_init = open(path.relpath('SQL/%s.mysql' % table_name) ).read()
		table_init_commands = table_init.split(';')
		try:
			for command in table_init_commands:
				db_cur.execute(command)
				db_con.commit()
		except Exception as e:
			print '%s.%s:\tERROR' % (db_schema,table_name)
			print e[1]
			sys.exit(2)
		print '%s.%s:\tCREATED' % (db_schema,table_name)
	db_cur.execute('''SHOW COLUMNS FROM `%s`''' % table_name)
	raw_headers = db_cur.fetchall()
	tmp_headers = []
	for row in raw_headers:
		tmp_headers.append(row[0])
		
	table_header = ','.join(tmp_headers)

def fetch_data(itemlist,locationID,debug=False):
	fetch_url = "%s%s" % (evecentral_url,fetch_type) 
	fetch_scope = query_locationType(locationID)
	itemid_str = ','.join(map(str,itemlist))
	POST_values = {
		'accept-encoding' : 'gzip',
		fetch_scope       : locationID,
		'user-agent'      : user_agent,
		'typeid'          : itemid_str
		}
	for tries in range (0,retry_limit):
		request = requests.post(fetch_url, data=POST_values)
		if request.status_code == requests.codes.ok:
			break
		else:
			print request.status_code
	
	return request.json()
		
def writeSQL(JSON_obj,locationID):
	insert_statement = '''INSERT INTO %s (%s) VALUES''' % (snapshot_table, table_header)
	
	for item_info in JSON_obj:
		for price_key,price_obj in item_info.iteritems():
			buy_or_sell = 0
			if price_key == "buy":
				buy_or_sell = 0
			elif price_key == "sell":
				buy_or_sell = 1
			else:
				continue
			best_price = None
			if buy_or_sell:
				best_price = price_obj['min']
			else:
				best_price = price_obj['max']
			
			insert_line = '''('%s','%s',%s,%s,'%s',%s,%s,%s,%s,%s)''' % (\
				commit_date,\
				commit_time,\
				price_obj['forQuery']['types'][0],\
				locationID,\
				query_locationType(locationID),\
				best_price,\
				price_obj['avg'],\
				price_obj['volume'])
				
def main():
	_initSQL(snapshot_table)
	
	item_list = fetch_typeIDs()
	
	locationID = 30000142
	request_limit = int(conf.get('CRON','evecentral_typelimit'))
	sub_list = []
	for itemid in item_list:
		sub_list.append(itemid)
		if len(sub_list) >= request_limit:
			return_JSON = fetch_data(sub_list,locationID)
			print return_JSON
			sys.exit()
			writeSQL(return_JSON,locationID)
			sub_list = []
	if len(sub_list) > 0:
		return_JSON = fetch_data(item_list,systemID)
		writeSQL(return_JSON)
			
	
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise