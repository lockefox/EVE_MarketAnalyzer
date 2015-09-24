import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
#from os import path, environ, getcwd
import urllib2
import httplib
import requests
import urllib
import MySQLdb 
#ODBC connector not supported on pi/ARM platform
from datetime import datetime, timedelta
import threading
import smtplib	#for emailing logs 

from ema_config import *
thread_exit_flag = False

db_con = None
db_cur = None

#### PROGRAM GLOBALS ####
itemlist = []
snapshot_table	= conf.get('TABLES', 'cron_evecentral')
logfile_name	= conf.get('CRON', 'evecentral_logfile') #add locationID to logfile name
evecentral_url	= conf.get('CRON', 'evecentral_baseURL')
fetch_type		= conf.get('CRON', 'evecentral_defaulttype')
live_table_range= conf.get('CRON', 'live_table_range')
table_header	= ''	#will be an issue if _initSQL is called multiple times
start_datetime	= datetime.utcnow()
commit_date		= start_datetime.strftime('%Y-%m-%d')
commit_time		= start_datetime.strftime('%H:%M:%S')
default_locationid = int(conf.get('CRON', 'evecentral_defaultlocationid'))
compressed_logging = int(conf.get('CRON', 'compressed_logging'))
script_dir_path = "%s/logs/" % os.path.dirname(os.path.realpath(__file__))
if not os.path.exists(script_dir_path):
	os.makedirs(script_dir_path)
#### MAIL STUFF ####
email_source		= str(conf.get('LOGGING', 'email_source'))
email_recipients	= str(conf.get('LOGGING', 'email_recipients'))
email_username		= str(conf.get('LOGGING', 'email_username'))
email_secret		= str(conf.get('LOGGING', 'email_secret'))
email_server		= str(conf.get('LOGGING', 'email_server'))
email_port			= str(conf.get('LOGGING', 'email_port'))

#Test to see if all email vars are initialized
#empty str() = False http://stackoverflow.com/questions/9573244/most-elegant-way-to-check-if-the-string-is-empty-in-python
bool_email_init = ( bool(email_source.strip()) and\
					bool(email_recipients.strip()) and\
					bool(email_username.strip()) and\
					bool(email_secret.strip()) and\
					bool(email_server.strip()) and\
					bool(email_port.strip()) )

def thread_print(msg):
	sys.stdout.write("%s\n" % msg)
	sys.stdout.flush()
	
def query_locationType(locationID, switch=False):
	#Returns supported query modifier.  Else blank to avoid bad calls
	digit = str(locationID)[:1]
	int_digit = int(digit)
	if int_digit == 1:
		if switch:
			return 'regionid'
		else:
			return 'regionlimit'
	#elif  int_digit == 2:
	#	None #constellation not supported
	elif int_digit == 3:
		if switch:
			return 'solarsystemid'
		else:
			return 'usesystem'
	elif int_digit == 6:
		if switch:
			return 'stationid'
		else:
			return 'usestation'
	else:
		if switch:
			return 'global'
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
	query_filename = '%s/SQL/%s.mysql' % (localpath, query_filename)
	item_query = open(query_filename).read()
	sde_cur.execute(item_query)
	raw_values = sde_cur.fetchall()
	
	return_list = []
	for row in raw_values:
		return_list.append(int(row[0]))
	return return_list
	
def writelog(locationID, message, push_email=False):
	logtime = datetime.utcnow()
	logtime_str = logtime.strftime('%Y-%m-%d %H:%M:%S')
	
	logfile = "%s%s-cron_evecentral" % (script_dir_path, locationID)
	log_msg = "%s::%s\n" % (logtime_str,message)
	if(compressed_logging):
		with gzip.open("%s.gz" % logfile,'a') as myFile:
			myFile.write(log_msg)
	else:
		with open("%s.log" % logfile,'a') as myFile:
			myFile.write(log_msg)
		
	if(push_email and bool_email_init):	#Bad day case
		#http://stackoverflow.com/questions/10147455/trying-to-send-email-gmail-as-mail-provider-using-python
		SUBJECT = '''cron_evecentral CRITICAL ERROR - %s''' % locationID
		BODY = message
		
		EMAIL = '''\From: {email_source}\nTo: {email_recipients}\nSubject: {SUBJECT}\n\n{BODY}'''
		EMAIL = EMAIL.format(
			email_source = email_source,
			email_recipients = email_recipients,
			SUBJECT = SUBJECT,
			BODY = BODY
			)
		try:
			mailserver = smtplib.SMTP(email_server,email_port)
			mailserver.ehlo()
			mailserver.starttls()
			mailserver.login(email_username, email_secret)
			mailserver.sendmail(email_source, email_recipients.split(', '), EMAIL)
			mailserver.close()
			writelog(locationID, "SENT email with critical failure to %s" % email_recipients, False)
		except:
			writelog(locationID, "FAILED TO SEND EMAIL TO %s" % email_recipients, False)

def _initSQL(table_name, locationID):
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
		writelog(locationID, '%s.%s:\tGOOD' % (db_schema,table_name))
	else:	#TODO: add override command to avoid 'drop table' command 
		table_init = open(path.relpath('SQL/%s.mysql' % table_name) ).read()
		table_init_commands = table_init.split(';')
		try:
			for command in table_init_commands:
				db_cur.execute(command)
				db_con.commit()
		except Exception as e:
			#TODO: push critical errors to email log (SQL error)
			writelog(locationID, '%s.%s:\tERROR: %s' % (db_schema, table_name, e[1]), True)
			sys.exit(2)
		writelog(locationID, '%s.%s:\tCREATED' % (db_schema, table_name))
	db_cur.execute('''SHOW COLUMNS FROM `%s`''' % table_name)
	raw_headers = db_cur.fetchall()
	tmp_headers = []
	for row in raw_headers:
		tmp_headers.append(row[0])
	
	global table_header
	table_header = ','.join(tmp_headers)

def fetch_data(itemlist, locationID, debug=False):
	if debug: print "\tfetch_data()"
	fetch_url = "%s%s" % (evecentral_url, fetch_type) 
	fetch_scope = query_locationType(locationID)
	itemid_str = ','.join(map(str, itemlist))
	if debug: print len(itemlist)
	if debug: print itemid_str
	POST_values = {
		'accept-encoding' : 'gzip',
		fetch_scope       : locationID,
		'user-agent'      : user_agent,
		'typeid'          : itemid_str
		}
	for tries in range (0,retry_limit):
		time.sleep(sleep_timer * tries)
		try:
			request = requests.post(fetch_url, 
				data=POST_values,
				timeout=(default_timeout,default_readtimeout))
			request.json()
		except requests.exceptions.ConnectionError as e:
			print 'connectionError %s' % e
			continue
		except requests.exceptions.ConnectTimeout as e:	
			print 'connectionTimeout %s' % e
			continue
		except requests.exceptions.ReadTimeout as e:	
			print 'readTimeout %s' % e
			continue
		except ValueError:
			print 'response not JSON'
			raise
		if request.status_code == requests.codes.ok:
			break
		else:
			print request.status_code
			continue
	else:
		error_msg = '''ERROR: unhandled exception fetching from EC
	url: {fetch_url}
	itemList: {itemid_str}
	Likely cases: 
		-- SDE/typeID missmatch
		-- Eve-Central is down'''
		error_msg = error_msg.format(
			fetch_url = fetch_url,
			itemid_str = itemid_str
			)
		writelog(locationID, error_msg, True)
		sys.exit(0)
		#TODO: push critical error to email log (connection error)
	return request.json()

def writeSQL(JSON_obj, locationID, debug=False):
	if debug: print "\twriteSQL()"
	insert_statement = '''INSERT IGNORE INTO %s (%s) VALUES''' % (snapshot_table, table_header)
		##INSERT IGNORE generates warnings for collisions, not errors
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
			
			insert_line = '''('%s','%s',%s,%s,'%s',%s,%s,%s,%s),''' % (\
				commit_date,\
				commit_time,\
				price_obj['forQuery']['types'][0],\
				locationID,\
				query_locationType(locationID, True),\
				buy_or_sell,\
				best_price,\
				price_obj['avg'],\
				price_obj['volume'])
			insert_statement = '%s%s' % (insert_statement, insert_line)
	insert_statement = insert_statement[:-1] #strip trailing ','
	if debug: print insert_statement
	try:
		db_cur.execute(insert_statement)
		db_con.commit()
	except Exception, e:
		error_str = '''ERROR: unable to insert into database
	Error msg: {exception_val}
	Commit str: {insert_statement}'''
		error_str = error_str.format(
			exception_val = e[1],
			insert_statement = insert_statement
			)
		writelog(locationID, error_str, True)
		sys.exit(2)

def integrity_check(locationID, debug=False):
	if debug: print "\tintegrity_check()"
	###CRITICAL LIST OF ITEMS FOR INTEGRITY CHECKING###
	#	mysql does not give a useful error code for full HDD 
	checklist = {
		29668	: "PLEX",
		34		: "Tritanium",
		35		: "Pyerite",
		36		: "Mexallon",
		37		: "Isogen",
		38		: "Nocxium",
		39		: "Zydrine",
		40		: "Megacyte",
		11399	: "Morphite",
		16670	: "Crystalline Carbonide",
		16671	: "Titanium Carbide",
		16672	: "Tungsten Carbide",
		16673	: "Fernite Carbide",
		16678	: "Sylramic Fibers",
		16679	: "Fullerides",
		16680	: "Phenolic Composites",
		16681	: "Nanotransistors",
		16682	: "Hypersynaptic Fibers",
		16683	: "Ferrogel",
		17317	: "Fermionic Condensates",
		33359	: "Photonic Metamaterials",
		33360	: "Terahertz Metamaterials",
		33361	: "Plasmonic Metamaterials",
		33362	: "Nonlinear Metamaterials",
		16633	: "Hydrocarbons",
		16634	: "Atmospheric Gases",
		16635	: "Evaporite Deposits",
		16636	: "Silicates",
		16637	: "Tungsten",
		16638	: "Titanium",
		16639	: "Scandium",
		16640	: "Cobalt",
		16641	: "Chromium",
		16642	: "Vanadium",
		16643	: "Cadmium",
		16644	: "Platinum",
		16646	: "Mercury",
		16647	: "Caesium",
		16648	: "Hafnium",
		16649	: "Technetium",
		16650	: "Dysprosium",
		16651	: "Neodymium",
		16652	: "Promethium",
		16653	: "Thulium"
	}
	
	typeList = ""
	for key,value in checklist.iteritems():
		typeList = "%s,%s" % (typeList, key)	#TODO: reduce checklist to typeids for simple join?
	typeList = typeList.lstrip(',')
	if debug: print typeList

	queryLen = len(checklist) * 4 #use LIMIT to help the integrity check come back faster?
	
	queryStr = '''SELECT * FROM {snapshot_table}
	WHERE locationID = {locationID}
	AND price_date = '{commit_date}'
	AND price_time = '{commit_time}'
	AND typeid IN ({typeList})
	AND price_best IS NOT NULL
	LIMIT {queryLen}'''
	queryStr = queryStr.format(
		snapshot_table = snapshot_table,
		locationID = locationID,
		commit_date = commit_date,
		commit_time = commit_time,
		typeList = typeList,
		queryLen = queryLen
		)
	if debug: print queryStr
	
	db_cur.execute(queryStr)
	checklist_server = db_cur.fetchall()
	if debug: print checklist_server
	if(len(checklist_server) != (len(checklist) * 2)):	#checklist_server = checklist*2 because buy_sell causes 2x rows
		print "Going down in flames"
		#TODO: push critical errors to email log (SQL missing critical data)
	else:
		if debug: print "\tintegrity_check() passed"

def cleanup_tables(locationID, live_range=live_table_range, debug=True):
	queryStr = '''SELECT * FROM {snapshot_table}
	WHERE price_date < (SELECT MAX(price_date) FROM [snapshot_table}) - INTERVAL {live_range} DAYS + 1'''
	queryStr = queryStr.format(
		snapshot_table = snapshot_table,
		live_range = live_range
		)
	
	if debug: print queryStr
	
	
	
def main():
	global snapshot_table
	locationID = default_locationid
	table_cleanup = False
	try:
		opts, args = getopt.getopt(sys.argv[1:],'h:l', ['locationid=','cleanup','table_override='])
	except getopt.GetoptError as e:
		print str(e)
		print 'unsupported argument'
		sys.exit()
	for opt, arg in opts:
		if opt == '--locationid':
			locationID = arg
		elif opt == '--optimize_table':
			table_cleanup = True
			writelog(locationID, "Executing table cleanup" % snapshot_table)
		elif opt == '--table_override':
			snapshot_table = arg
			writelog(locationID, "write table changed to: `%s`" % snapshot_table)
		else:
			assert False

	_initSQL(snapshot_table, locationID)
	
	item_list = fetch_typeIDs()
		
	request_limit = int(conf.get('CRON', 'evecentral_typelimit'))
	sub_list = []
	step_count = 1
	for itemid in item_list:
		sub_list.append(itemid)
		if len(sub_list) >= request_limit:
			writelog(locationID, "fetching list STEP: %s" % (step_count))
			return_JSON = fetch_data(sub_list, locationID)
			writelog(locationID, "fetching list: SUCCESS")
			writeSQL(return_JSON, locationID)
			writelog(locationID, "writing list to DB: SUCCESS")
			sub_list = []
			step_count += 1
	if len(sub_list) > 0:
		writelog(locationID, "fetching list STEP: %s" % (step_count))
		return_JSON = fetch_data(sub_list, locationID)
		writelog(locationID, "fetching list: SUCCESS")
		writeSQL(return_JSON, locationID)
		writelog(locationID, "writing list to DB: SUCCESS")
		step_count += 1 
	integrity_check(locationID)
	writelog(locationID, "integrity_check() passed", False)
	
	if table_cleanup: 
		cleanup_tables(locationID)
		writelog(locationID, "cleanup_tables() passed", False)

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise