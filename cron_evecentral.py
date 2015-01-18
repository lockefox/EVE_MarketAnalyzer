import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path, environ
import urllib2
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

def thread_print(msg):
	sys.stdout.write("%s\n" % msg)
	sys.stdout.flush()
	
def query_locationType(locationID):
	#Returns supported query modifier.  Else blank to avoid bad calls
	digit = str(locationID)[-1]
	int_digit = int(digit)
	
	if int_digit == 1:
		return 'regionlimit=%s' % locationID
	#elif  int_digit == 2:
	#	None #constellation not supported
	elif int_digit == 3:
		return 'usesystem=%s' % locationID
	elif int_digit == 6:
		return 'usestation=%s' % locationID
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
	print item_query
	sde_cur.execute(item_query)
	raw_values = sde_cur.fetchall()
	
	return_list = []
	for row in raw_values:
		return_list.append(row[0])
	return return_list
	
def writelog(locationID, message):
	None
	
def _initSQL(table_name):
	global db_con, db_cur
	
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

def fetch_data(itemlist,locationID,debug=False):
	fetch_scope = query_locationType(locationID)
	#POST_values = {
	#	'accept-encoding' : 'gzip',
	#	fetch_scope       : locationID,
	#	'user-agent'      : user_agent,
	#	'typeid'          : ','.join(itemlist)
	#	}
	#
	#payload = urllib.urlencode(POST_values)
	#request = urllib2.Request(evecentral_url,payload)
	
	real_query = '%s?%s=%s&typeid=%s' % (evecentral_url,fetch_scope,locationID,','.join(itemlist))
	#if debug: thread_print( real_query )
	
	request = urllib2.Request(evecentral_url)
	request.add_header('Accept-Encoding','gzip')
	request.add_header('User-Agent',user_agent)
	request.add_header(fetch_scope,locationID)
	request.add_header('typeid',','.join(itemlist))
	
	headers = {}
	fatal_error = True
	for tries in range (0,retry_limit):
		time.sleep(sleep_timer*tries)
		try:
			opener = urllib2.build_opener()
			raw_response = opener.open(request)
			headers = raw_response.headers
			response = raw_response.read()
		except urllib2.HTTPError as e:
			if e.code == 503: 
				time.sleep(random.random()/8.0)
			else:
				print 'HTTPError:%s %s' % (e,real_query) 
			fatal_error = False
			continue
		except urllib2.URLError as e:
			print 'URLError:%s %s' % (e,real_query) 
			fatal_error = True
			continue
		except httplib.HTTPException as e:
			print 'HTTPException:%s %s' % (e, real_query) 
			fatal_error = True
			continue
		except socket.error as e:
			print 'Socket Error:%s %s' % (e,real_query) 
			fatal_error = True
			continue
		
		do_gzip = headers.get('Content-Encoding','') == 'gzip'
				
		if do_gzip:
			try:
				buf = StringIO.StringIO(response)
				zipper = gzip.GzipFile(fileobj=buf)
				return_result = json.load(zipper)
			except ValueError as e:
				print "Empty response: Retry %s" % real_query 
				fatal_error = True
				continue
			except IOError as e:
				print "gzip unreadable: Retry %s" % real_query 
				fatal_error = True
				continue
			else:
				break
		else:
			return_result = json.loads(response)
			break
	else:
		print headers 
		if fatal_error: sys.exit(2)
		return {'items':[]}
	
	return return_result
def main():
	_initSQL(snapshot_table)
	
	item_list = fetch_typeIDs()
	print item_list
	
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise