import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket, platform
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

db_partcipants	= None
db_fits					= None
db_crestInfo		= None
db_losses				= None
db_locations		= None

##### GLOBAL VARS #####
script_pid = ""
debug = False
tableName_participants	= conf.get('TABLES', 'zkb_participants')
tableName_fits        	= conf.get('TABLES', 'zkb_fits')
tableName_losses       	= conf.get('TABLES', 'zkb_trunc_stats')
tableName_crestInfo			= conf.get('TABLES', 'zkb_crest_info')
tableName_location			= conf.get('TABLES', 'zkb_location')
scriptName = "cron_zkb" #used for PID locking

compressed_logging	= int(conf.get('CRON', 'compressed_logging'))
zkb_exception_limit	= int(conf.get('CRON', 'zkb_exception_limit'))
redisq_url					= conf.get('CRON', 'zkb_redis_link')
retry_sleep					= int(conf.get('ZKB', 'default_sleep'))
script_dir_path = "%s/logs/" % os.path.dirname(os.path.realpath(__file__))
if not os.path.exists(script_dir_path):
	os.makedirs(script_dir_path)

#### MAIL STUFF ####
email_source			= str(conf.get('LOGGING', 'email_source'))
email_recipients	= str(conf.get('LOGGING', 'email_recipients'))
email_username		= str(conf.get('LOGGING', 'email_username'))
email_secret			= str(conf.get('LOGGING', 'email_secret'))
email_server			= str(conf.get('LOGGING', 'email_server'))
email_port				= str(conf.get('LOGGING', 'email_port'))

class zkbException(Exception):
	def __init__ (self, exception, message):
		self.message = message
		self.source_error = exception
	def __str__ (self):
		return "%s: %s" % (self.message, self.source_error)
	def __nonzero__ (self):
		return True
		
class DB_handle (object):
	#Designed to hold SQL connection info
	#Though, most tables should be on same schema?
	
	def __init__ (self, db_con, db_cur, table_name):
		self.db_con = db_con
		self.db_cur = db_cur
		self.table_name = table_name
		self.raw_headers = []
		self.raw_headers = self.fetch_table_headers()
		self.table_headers = ','.join(self.raw_headers)
	def fetch_table_headers(self):
		self.db_cur.execute('''SHOW COLUMNS FROM `%s`''' % self.table_name) #MySQL only
		raw_headers = self.db_cur.fetchall()
		tmp_headers = []
		for row in raw_headers:
			tmp_headers.append(row[0])
		return tmp_headers
	def __str__ (self):
		return self.table_name

def writelog(pid, message, push_email=False):
	logtime = datetime.utcnow()
	logtime_str = logtime.strftime('%Y-%m-%d %H:%M:%S')
	
	logfile = "%s%s-cron_zkb" % (script_dir_path, pid)
	log_msg = "%s::%s\n" % (logtime_str,message)
	if(compressed_logging):
		with gzip.open("%s.gz" % logfile,'a') as myFile:
			myFile.write(log_msg)
	else:
		with open("%s.log" % logfile,'a') as myFile:
			myFile.write(log_msg)
		
	if(push_email and bool_email_init):	#Bad day case
		#http://stackoverflow.com/questions/10147455/trying-to-send-email-gmail-as-mail-provider-using-python
		SUBJECT = '''cron_zkb CRITICAL ERROR - %s''' % pid
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
			writelog(pid, "SENT email with critical failure to %s" % email_recipients, False)
		except:
			writelog(pid, "FAILED TO SEND EMAIL TO %s" % email_recipients, False)

def get_lock(process_name):
	#Stolen from: http://stackoverflow.com/a/7758075
	global lock_socket   # Without this our lock gets garbage collected
	lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
	try:
		lock_socket.bind('\0' + process_name)
		writelog(pid, "PID-Lock acquired")
	except socket.error:
		writelog(pid, "PID already locked.  Quitting")
		sys.exit()

def _initSQL(table_name, pid=script_pid):
	#global db_con, db_cur
	try:
		db_con = MySQLdb.connect(
			host   = db_host,
			user   = db_user,
			passwd = db_pw,
			port   = db_port,
			db     = db_schema)
		db_cur = db_con.cursor()
	except OperationalError as e:	#Unable to connect to SQL instance
		writelog(pid, '%s.%s:\tERROR: %s' % (db_schema, table_name, e[1]), True)
	
	db_cur.execute('''SHOW TABLES LIKE \'%s\'''' % table_name)
	db_exists = len(db_cur.fetchall())
	if db_exists:
		writelog(pid, '%s.%s:\tGOOD' % (db_schema,table_name))
	else:	#TODO: add override command to avoid 'drop table' command 
		table_init = open(path.relpath('SQL/%s.mysql' % table_name) ).read()
		table_init_commands = table_init.split(';')
		try:
			for command in table_init_commands:
				db_cur.execute(command)
				db_con.commit()
		except Exception as e: #Unable to create desired table
			writelog(pid, '%s.%s:\tERROR: %s' % (db_schema, table_name, e[1]), True)
			sys.exit(2)
		writelog(pid, '%s.%s:\tCREATED' % (db_schema, table_name))
	db_cur.execute('''SHOW COLUMNS FROM `%s`''' % table_name)
	raw_headers = db_cur.fetchall()
	tmp_headers = []
	for row in raw_headers:
		tmp_headers.append(row[0])
	
	db_obj = DB_handle(db_con, db_cur, table_name)	#put db parts in a class for better portability
	return db_obj

def fetch_data(pid, debug=False):
	#if debug: print "\tfetch_data()"
	fetch_url = redisq_url
	POST_values = {
		'accept-encoding' : 'gzip',
		'user-agent'      : user_agent,
		}
	last_error = False
	for tries in range (0,retry_limit):
		time.sleep(sleep_timer * tries)
		try:
			request = requests.post(fetch_url, 
				data=POST_values,
				timeout=(default_timeout,default_readtimeout)
				)			
		except requests.exceptions.ConnectionError as e:
			last_error = zkbException(e, 'requests.ConnectionError tries=%s' % tries)
			writelog( pid, last_error )
			continue
		except requests.exceptions.ConnectTimeout as e:	
			last_error =  zkbException(e, 'requests.ConnectionTimeout tries=%s' % tries)
			write_log( pid, last_error )
			continue
		except requests.exceptions.ReadTimeout as e:	
			last_error = zkbException(e, 'requests.ReadTimeout tries=%s' % tries)
			writelog( pid, last_error )
			continue
		
		if request.status_code == requests.codes.ok:
			try:
				request.json()
			except ValueError as e:
				last_error = zkbException(e, 'response not JSON tries=%s' % tries)
				writelog( pid, last_error )
				continue
			break	#if all OK, break out of error checking
		else:
			last_error = zkbException(request.status_code, 'bad status code tries=%s' % tries)
			writelog( pid, last_error )
			continue
	else:
		raise last_error	#let main handle final crash/retry logic 
	##	error_msg = '''ERROR: unhandled exception fetching from EC
	##url: {fetch_url}
	##errorMsg: {last_error}
	##Likely cases: 
	##	-- TODO'''
	##	error_msg = error_msg.format(
	##		fetch_url  = fetch_url,
	##		last_error = last_error
	##		)
	##	writelog(pid, error_msg, True)
	##	sys.exit(0)
	return request.json()

def test_killInfo (kill_obj, pid=script_pid, debug=False):
	#if debug: print "save_killInfo()"
	kill_info = kill_obj['package']
	if kill_info == None:
		writelog(pid, "ERROR: empty response")
		raise zkbException(None, "empty response")
		#return "empty response"
	try:	#check that the critical pieces of any kill are in tact
		killID		= int(kill_obj['package']['killID'])
		hash 			= kill_info['zkb']['hash']
		killTime	= kill_info['killmail']['killTime']
	except KeyError as e:
		writelog(pid, "ERROR: critical key check failed: %s" % e)
		raise zkbException(e, "ERROR: critical key check failed")
		#return e #let main handle final crash/retry logic 	
	except TypeError as e:
		writelog(pid, "ERROR: critical key check failed: %s" % e)
		raise zkbException(e, "ERROR: critical key check failed")
		#return e #let main handle final crash/retry logic 		
	if debug: print "%s @ %s" % (killID, killTime)
		
	try:
		killTime_datetime = datetime.strptime(killTime, "%Y.%m.%d %H:%M:%S") #2015.12.06 02:12:30
	except ValueError as e:
		writelog(pid, "ERROR: unable to convert `killTime`:%s %s" % (killTime, e))	
		raise zkbException(e, "ERROR: unable to convert `killTime`:%s" % killTime)
		#raise e #let main handle final crash/retry logic 
		#return e #let main handle final crash/retry logic 
	
	writelog(pid, "killID: %s PASS: critical key check" % killID)
	return '' #return empty string

def process_participants(kill_data, dbObj, pid=script_pid, debug=False):
	## Global vars (every commit)
	try:
		killID 				= int(kill_data['package']['killID'])
		solarSystemID	= int(kill_data['package']['killmail']['solarSystem']['id'])
		killTime_str	=     kill_data['package']['killmail']['killTime']
		#locationID		= int(kill_data['package']['zkb']['locationID'])
		killTime_datetime = datetime.strptime(killTime_str, "%Y.%m.%d %H:%M:%S") #2015.12.06 02:12:30
		killTime = killTime_datetime.strftime("%Y-%m-%d %H:%M:%S")
	except KeyError as e:
		raw_json = json.dumps(kill_data, sort_keys=True, indent=4, separators=(',', ': '))
		writelog(pid, "JSON error %s: %s" % (e,raw_json), True)
	except TypeError as e:	#TODO: nasty crash error should be handled by test_killInfo()
		raw_json = json.dumps(kill_data, sort_keys=True, indent=4, separators=(',', ': '))
		writelog(pid, "JSON error %s: %s" % (e,raw_json), True)
	## Victim Info
	isVictim = 1
	try:
		characterID		= int(kill_data['package']['killmail']['victim']['character']['id'])
	except KeyError as e:
		characterID		= -1 #POS equipment doesn't have a characterID 
	try:
		shipTypeID		= int(kill_data['package']['killmail']['victim']['shipType']['id'])
		weaponType		= 'NULL'
		damage				= int(kill_data['package']['killmail']['victim']['damageTaken'])		
		corporationID	= int(kill_data['package']['killmail']['victim']['corporation']['id'])
	except Exception as e:
		raw_json = json.dumps(kill_data, sort_keys=True, indent=4, separators=(',', ': '))
		writelog(pid, "JSON error %s: %s" % (e,raw_json), True)
	try:
		allianceID = int(kill_data['package']['killmail']['victim']['alliance']['id'])
	except KeyError as e:
		allianceID = 'NULL'
	try:
		factionID = int(kill_data['package']['killmail']['victim']['faction']['id'])
	except KeyError as e:
		factionID = 'NULL'
	finalBlow = 'NULL'
	
	## Commit str start
	base_commit_str = '''INSERT IGNORE INTO {table_name} ({table_headers}) VALUES'''
	base_commit_str = base_commit_str.format(
		table_name 		= dbObj.table_name,
		table_headers	= dbObj.table_headers
		)
		
	victimInfo = \
	'''({killID},{solarSystemID},'{kill_time}',{isVictim},{shipTypeID},{weaponType},{damage},{characterID},{corporationID},{allianceID},{factionID},{finalBlow})'''
	victimInfo = victimInfo.format(
		killID 				= killID,
		solarSystemID = solarSystemID,
		kill_time			= killTime,
		isVictim			= isVictim,
		shipTypeID		= shipTypeID,
		weaponType		= weaponType,
		damage				= damage,
		characterID		= characterID,
		corporationID = corporationID,
		allianceID		= allianceID,
		factionID			= factionID,
		finalBlow			= finalBlow
		)
	#if debug: print victimInfo
	
	commit_str = '''{base_commit_str} {victimInfo}'''
	commit_str = commit_str.format(
		base_commit_str = base_commit_str,
		victimInfo			= victimInfo
		)
		
	for attackerObj in kill_data['package']['killmail']['attackers']:
		isVictim = 0 
		try:
			shipTypeID		= int(attackerObj['shipType']['id'])
		except KeyError as e:
			shipTypeID		= -1 #shiptype can be blank on a record.  Ship lost in combat
		try:
			weaponType		= int(attackerObj['weaponType']['id'])
		except KeyError as e:
			weaponType		= 'NULL'
		try:
			characterID		= int(attackerObj['character']['id'])
			corporationID	= int(attackerObj['corporation']['id'])
		except KeyError as e:
			characterID		= -1	#NPC in killmail
			corporationID	= -1
		try:
			damage				= int(attackerObj['damageDone'])	
			finalBlow = int(attackerObj['finalBlow'])			
		except Exception as e:
			raw_json = json.dumps(kill_data, sort_keys=True, indent=4, separators=(',', ': '))
			raw_attackers = json.dumps(attackerObj, sort_keys=True, indent=4, separators=(',', ': '))
			writelog(pid, "JSON error %s: %s\n%s" % (e,raw_json,raw_attackers), True)
		try:
			allianceID = int(attackerObj['alliance']['id'])
		except KeyError as e:
			allianceID = 'NULL'
		try:
			factionID = int(attackerObj['faction']['id'])
		except KeyError as e:
			factionID = 'NULL'
		
		
		attackerInfo = \
		'''({killID},{solarSystemID},'{kill_time}',{isVictim},{shipTypeID},{weaponType},{damage},{characterID},{corporationID},{allianceID},{factionID},{finalBlow})'''
		attackerInfo = attackerInfo.format(
			killID 				= killID,
			solarSystemID = solarSystemID,
			kill_time			= killTime,
			isVictim			= isVictim,
			shipTypeID		= shipTypeID,
			weaponType		= weaponType,
			damage				= damage,
			characterID		= characterID,
			corporationID = corporationID,
			allianceID		= allianceID,
			factionID			= factionID,
			finalBlow			= finalBlow
			)
		commit_str = "%s, %s" % (commit_str, attackerInfo)
	
	writeSQL(commit_str, dbObj, script_pid, debug)
	writelog(pid, "killID: %s -- Participants written" % killID)
	
def process_fits(kill_data, dbObj, pid=script_pid, debug=False):
	try:
		killID 				= int(kill_data['package']['killID'])
		shipTypeID		= int(kill_data['package']['killmail']['victim']['shipType']['id'])
	except KeyError as e:
		raw_json = json.dumps(kill_data, sort_keys=True, indent=4, separators=(',', ': '))
		writelog(pid, "JSON error %s: %s" % (e,raw_json), True)
	base_commit_str = '''INSERT IGNORE INTO {table_name} ({table_headers}) VALUES'''
	base_commit_str = base_commit_str.format(
		table_name 		= dbObj.table_name,
		table_headers	= dbObj.table_headers
		)
	ship_commit_str = \
	'''({killID},{shipTypeID},{typeID},{flag},{qtyDropped},{qtyDestroyed},{singleton})'''
	ship_commit_str = ship_commit_str.format(
		killID 				= killID,
		shipTypeID		= shipTypeID,
		typeID				= shipTypeID,
		flag					= -1,
		qtyDropped		= 0,
		qtyDestroyed	= 1,
		singleton			= 'NULL'
		)
	commit_str = '''{base_commit_str} {ship_commit_str}'''
	commit_str = commit_str.format(
		base_commit_str = base_commit_str,
		ship_commit_str = ship_commit_str
		)
		
	for itemObj in kill_data['package']['killmail']['victim']['items']:
		try:
			typeID	= int(itemObj['itemType']['id'])
			flag		= int(itemObj['flag'])
			singleton	= int(itemObj['singleton'])
		except KeyError as e:
			raw_json = json.dumps(kill_data, sort_keys=True, indent=4, separators=(',', ': '))
			raw_items = json.dumps(itemObj, sort_keys=True, indent=4, separators=(',', ': '))
			writelog(pid, "JSON error %s: %s\n%s" % (e,raw_json,raw_attackers), True)
		try:
			qtyDropped	= int(itemObj['quantityDropped'])
		except KeyError as e:
			qtyDropped	= 0
		try:
			qtyDestroyed	= int(itemObj['quantityDestroyed'])
		except KeyError as e:
			qtyDestroyed	= 0
		
		itemInfo = \
		'''({killID},{shipTypeID},{typeID},{flag},{qtyDropped},{qtyDestroyed},{singleton})'''
		itemInfo = itemInfo.format(
			killID 				= killID,
			shipTypeID		= shipTypeID,
			typeID				= typeID,
			flag					= flag,
			qtyDropped		= qtyDropped,
			qtyDestroyed	= qtyDestroyed,
			singleton			= singleton
			)
		commit_str = "%s, %s" % (commit_str, itemInfo)
	writeSQL(commit_str, dbObj, script_pid, debug)
	writelog(pid, "killID: %s -- Fits written" % killID)
	
def process_losses(kill_data, dbObj, pid=script_pid, debug=False):
	try:
		killID 				= int(kill_data['package']['killID'])
		solarSystemID	= int(kill_data['package']['killmail']['solarSystem']['id'])
		killTime_str	=     kill_data['package']['killmail']['killTime']
		killTime_datetime = datetime.strptime(killTime_str, "%Y.%m.%d %H:%M:%S") #2015.12.06 02:12:30
		killTime = killTime_datetime.strftime("%Y-%m-%d %H:%M:%S")
		shipTypeID		= int(kill_data['package']['killmail']['victim']['shipType']['id'])
		damage				= int(kill_data['package']['killmail']['victim']['damageTaken'])		
		corporationID	= int(kill_data['package']['killmail']['victim']['corporation']['id'])
	except KeyError as e:
		raw_json = json.dumps(kill_data, sort_keys=True, indent=4, separators=(',', ': '))
		writelog(pid, "JSON error %s: %s" % (e,raw_json), True)	
	try:
		characterID		= int(kill_data['package']['killmail']['victim']['character']['id'])
	except KeyError as e:
		characterID		= -1 #POS equipment doesn't have a characterID 
	try:
		allianceID = int(kill_data['package']['killmail']['victim']['alliance']['id'])
	except KeyError as e:
		allianceID = 'NULL'
	try:
		factionID = int(kill_data['package']['killmail']['victim']['faction']['id'])
	except KeyError as e:
		factionID = 'NULL'
	try:
		locationID = int(kill_data['package']['zkb']['locationID'])
	except KeyError as e:
		locationID = 'NULL'
	try:
		participants = len(kill_data['package']['killmail']['attackers'])
	except KeyError as e:
		participants = 0 #TODO: ERROR case?
	totalValue = 'NULL' #TODO: price processing in POST 
	
	base_commit_str = '''INSERT IGNORE INTO {table_name} ({table_headers}) VALUES'''
	base_commit_str = base_commit_str.format(
		table_name 		= dbObj.table_name,
		table_headers	= dbObj.table_headers
		)
	lossesInfo = \
	'''({killID},{solarSystemID},'{kill_time}',{shipTypeID},{damage},{characterID},{corporationID},{allianceID},{factionID},{totalValue},{participants},{locationID})'''
	lossesInfo = lossesInfo.format(
		killID				= killID,
		solarSystemID	= solarSystemID,
		kill_time			= killTime,
		shipTypeID		= shipTypeID,
		damage				= damage,
		characterID		= characterID,
		corporationID	= corporationID,
		allianceID		= allianceID,
		factionID			= factionID,
		totalValue		= totalValue,
		participants	= participants,
		locationID		= locationID
		)
	
	commit_str = '''{base_commit_str} {lossesInfo}'''
	commit_str = commit_str.format(
		base_commit_str = base_commit_str,
		lossesInfo			= lossesInfo
		)
	writeSQL(commit_str, dbObj, script_pid, debug)
	writelog(pid, "killID: %s -- Losses written" % killID)
	
def process_locations(kill_data, dbObj, pid=script_pid, debug=False):
	try:
		killID 				= int(kill_data['package']['killID'])	
	except KeyError as e:
		raw_json = json.dumps(kill_data, sort_keys=True, indent=4, separators=(',', ': '))
		writelog(pid, "JSON error %s: %s" % (e,raw_json), True)	
	try:
		locationID = int(kill_data['package']['zkb']['locationID'])
	except KeyError as e:
		locationID = 'NULL'
	try:
		x = kill_data['package']['killmail']['victim']['position']['x']
		y = kill_data['package']['killmail']['victim']['position']['y']
		z = kill_data['package']['killmail']['victim']['position']['z']
	except KeyError as e:
		x = 'NULL'
		y = 'NULL'
		z = 'NULL'
	
	base_commit_str = '''INSERT IGNORE INTO {table_name} ({table_headers}) VALUES'''
	base_commit_str = base_commit_str.format(
		table_name 		= dbObj.table_name,
		table_headers	= dbObj.table_headers
		)
	locationInfo = \
	'''({killID},{locationID},{x},{y},{z})'''
	locationInfo = locationInfo.format(
		killID			= killID,
		locationID	= locationID,
		x						= x,
		y						= y,
		z						= z,
		)
	
	commit_str = '''{base_commit_str} {locationInfo}'''
	commit_str = commit_str.format(
		base_commit_str = base_commit_str,
		locationInfo			= locationInfo
		)
	
	writeSQL(commit_str, dbObj, script_pid, debug)
	writelog(pid, "killID: %s -- Locations written" % killID)
	
def process_crestInfo(kill_data, dbObj, pid=script_pid, debug=False):
	datetime_now = datetime.utcnow()
	record_processed = datetime_now.strftime("Y-%m-%d %H:%M:%S")
	try:
		killID				= int(kill_data['package']['killID'])
		hash					= 		kill_data['package']['zkb']['hash']
		killTime_str	=     kill_data['package']['killmail']['killTime']
		killTime_datetime = datetime.strptime(killTime_str, "%Y.%m.%d %H:%M:%S") #2015.12.06 02:12:30
		killTime = killTime_datetime.strftime("%Y-%m-%d %H:%M:%S")
	except KeyError as e:
		raw_json = json.dumps(kill_data, sort_keys=True, indent=4, separators=(',', ': '))
		writelog(pid, "JSON error %s: %s" % (e,raw_json), True)	
		
	base_commit_str = '''INSERT IGNORE INTO {table_name} ({table_headers}) VALUES'''
	base_commit_str = base_commit_str.format(
		table_name 		= dbObj.table_name,
		table_headers	= dbObj.table_headers
		)
	crestInfo = \
	'''({killID},'{hash}','{kill_time}','{record_processed}')'''
	crestInfo = crestInfo.format(
		killID						= killID,
		hash							= hash,
		kill_time					= killTime,
		record_processed	= record_processed
		)
	
	commit_str = '''{base_commit_str} {crestInfo}'''
	commit_str = commit_str.format(
		base_commit_str = base_commit_str,
		crestInfo				= crestInfo
		)
	writeSQL(commit_str, dbObj, script_pid, debug)
	writelog(pid, "killID: %s -- crestInfo written" % killID)

def writeSQL(commit_str, dbObj, pid=script_pid, debug=False):
	#if debug: print "%s: %s" % (dbObj, commit_str)
	try:
		dbObj.db_cur.execute(commit_str)
		dbObj.db_con.commit()
	except Exception, e:
		error_str = '''ERROR: unable to insert into database
	Error msg: {exception_val}
	Commit str: {commit_str}'''
		error_str = error_str.format(
			exception_val = e[1],
			commit_str = commit_str
			)
		writelog(pid, error_str, True)
		sys.exit(2)
		
def main():
	table_cleanup = False
	global script_pid, debug
	script_pid = str(os.getpid())
	
#### Get CLI options ####
	try:
		opts, args = getopt.getopt(sys.argv[1:],'h:l', ['cleanup','debug'])
	except getopt.GetoptError as e:
		print str(e)
		print 'unsupported argument'
		sys.exit()
	for opt, arg in opts:
		if opt == '--cleanup':
			table_cleanup = True
			writelog(script_pid, "Executing table cleanup" % snapshot_table)
		elif opt == "--debug":
			debug = True
		else:
			assert False
			
#### Figure out if program is already running ####
	if platform.system() == "Windows":
		print "PID Locking not supported on windows"
		if debug: print "--DEBUG MODE-- Overriding PID lock"
		else: sys.exit(0)
	else:
		get_lock(scriptName) 
		
#### Set up db connections for query/write ####
	global db_partcipants
	db_partcipants = _initSQL(tableName_participants, script_pid)
	global db_fits
	db_fits = _initSQL(tableName_fits, script_pid)
	global db_losses
	db_losses = _initSQL(tableName_losses, script_pid)
	global db_crestInfo
	db_crestInfo = _initSQL(tableName_crestInfo, script_pid)
	global db_locations
	db_locations = _initSQL(tableName_location, script_pid)
	#db_partcipants.db_cur.execute('''SHOW COLUMNS FROM `%s`''' % tableName_participants)
	#print db_partcipants.db_cur.fetchall()
	
#### Fetch zkb redisq data ####
	package_null = False
	kills_processed = 0
	fail_count = 0
	while (not package_null):
		caught_exception = ''
		try:
			kill_data = fetch_data(script_pid, debug)
		except Exception as e:
			caught_exception = e
			
		try:
			custom_exception = test_killInfo(kill_data, script_pid, debug)
			caught_exception = "%s%s" % (caught_exception, custom_exception)
			#TODO: write custom Exception class for critical errors
		except Exception as e:
			caught_exception = e
		
		process_participants(kill_data, db_partcipants, script_pid, debug)
		process_fits(kill_data, db_fits, script_pid, debug)
		process_losses(kill_data, db_losses, script_pid, debug)
		process_locations(kill_data, db_locations, script_pid, debug)
		process_crestInfo(kill_data, db_crestInfo, script_pid, debug)
		
		if caught_exception:	#check to see if parsing should end
			if kills_processed == 0:
				time.sleep(retry_sleep)
				if fail_count >= zkb_exception_limit:
					error_msg = '''EXCEPTION FOUND: no kills_processed, and fail_count exceeded.  Probable error.
	kills_processed = {kills_processed}
	fail_count = {fail_count}
	exception = {caught_exception}'''
					error_msg = error_msg.format(
						kills_processed = kills_processed,
						fail_count = fail_count,
						caught_exception = caught_exception
						)
					writelog(script_pid, error_msg, True)
					
				writelog(script_pid, "EXCEPTION FOUND: but kills_processed = %s, retry case: %s" % (kills_processed,caught_exception))
				#kills_processed += 1
				fail_count += 1 
				continue
			elif kills_processed > 0:
				writelog(script_pid, "EXCEPTION FOUND: kills_processed = %s, sleep case %s" % (kills_processed,caught_exception))
			#Clean up connections#
				db_partcipants.db_con.close()
				db_fits.db_con.close()
				db_losses.db_con.close()
				db_crestInfo.db_con.close()
				db_locations.db_con.close()
			#quit normally#
				sys.exit(0)
			else:
				writelog(script_pid, "EXCEPTION FOUND: invalid value for `kills_processed`=%s, exception=%s" % (kills_processed, caught_exception), True)
			
		kills_processed += 1 
		##kill_data = fetch_data(script_pid, debug)
		##empty_check = ""
		##try:
		##	empty_check = kill_data['package'].lower()
		##except AttributeError as e:
		##	print "not 'null'"
		##if empty_check == 'null':	#TODO: reverse logic?
		##	if kills_processed > 0:
		##		writelog(pid, "Kill processing complete: kills processed=%s" % kills_processed)
		##	elif kills_processed == 0:
		##		writelog(pid, "Blank returned as first query.  Retrying")
		##	else:
		##		if debug: print "invalid value for `kills_processed`=%s" % kills_processed
		##		writelog(pid, "invalid value for `kills_processed`=%s" % kills_processed, True)
		##
		##kills_processed += 1
		##if debug: print kill_data['package']['killID']
		
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise