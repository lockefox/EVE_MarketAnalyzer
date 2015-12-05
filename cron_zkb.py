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

db_partcipants = None
db_fits = None
#db_con = None
#db_cur = None

##### GLOBAL VARS #####
script_pid = ""
debug = False
tableName_participants	= conf.get('TABLES', 'zkb_participants')
tableName_fits        	= conf.get('TABLES', 'zkb_fits')
tableName_losses       	= conf.get('TABLES', 'zkb_trunc_stats')
scriptName = "cron_zkb" #used for PID locking

compressed_logging = int(conf.get('CRON', 'compressed_logging'))
script_dir_path = "%s/logs/" % os.path.dirname(os.path.realpath(__file__))
if not os.path.exists(script_dir_path):
	os.makedirs(script_dir_path)

class DB_handle (object):
	#Designed to hold SQL connection info
	#Though, most tables should be on same schema?
	def __init__ (self, db_con, db_cur, table_name):
		self.db_con = db_con
		self.db_cur = db_cur
		self.table_name = table_name
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
    global lock_socket   # Without this our lock gets garbage collected
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        print 'I got the lock'
    except socket.error:
        print 'lock exists'
        sys.exit()

				
def _initSQL(table_name, pid=script_pid):
	#global db_con, db_cur
	
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
		writelog(pid, '%s.%s:\tGOOD' % (db_schema,table_name))
	else:	#TODO: add override command to avoid 'drop table' command 
		table_init = open(path.relpath('SQL/%s.mysql' % table_name) ).read()
		table_init_commands = table_init.split(';')
		try:
			for command in table_init_commands:
				db_cur.execute(command)
				db_con.commit()
		except Exception as e:
			#TODO: push critical errors to email log (SQL error)
			writelog(pid, '%s.%s:\tERROR: %s' % (db_schema, table_name, e[1]), True)
			sys.exit(2)
		writelog(pid, '%s.%s:\tCREATED' % (db_schema, table_name))
	db_cur.execute('''SHOW COLUMNS FROM `%s`''' % table_name)
	raw_headers = db_cur.fetchall()
	tmp_headers = []
	for row in raw_headers:
		tmp_headers.append(row[0])
	
	global table_header
	table_header = ','.join(tmp_headers)
	db_obj = DB_handle(db_con, db_cur, table_name)	#put db parts in a class for better portability
	return db_obj
	
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
			writelog(pid, "Executing table cleanup" % snapshot_table)
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
	
	#db_partcipants.db_cur.execute('''SHOW COLUMNS FROM `%s`''' % tableName_participants)
	#print db_partcipants.db_cur.fetchall()
	
#### Fetch zkb redisq data ####
	package_null = False
	while (not package_null):
		None
	
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise