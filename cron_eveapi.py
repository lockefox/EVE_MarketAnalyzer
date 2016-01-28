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
from eveapi import eveapi

from ema_config import *
thread_exit_flag = False

gDebug = False
gLogID = ''
scriptName = "cron_eveapi"
script_pid = ""
script_dir_path = "%s/logs/" % os.path.dirname(os.path.realpath(__file__))

cachedUntil_logName = "cron_eveapi_cachedUntil.json"
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
		writelog(pid, script_dir_path, scriptName, '%s.%s:\tERROR: %s' % (db_schema, table_name, e[1]), True)
	
	db_cur.execute('''SHOW TABLES LIKE \'%s\'''' % table_name)
	db_exists = len(db_cur.fetchall())
	if db_exists:
		writelog(pid, script_dir_path, scriptName, '%s.%s:\tGOOD' % (db_schema,table_name))
	else:	#TODO: add override command to avoid 'drop table' command 
		table_init = open(path.relpath('SQL/%s.mysql' % table_name) ).read()
		table_init_commands = table_init.split(';')
		try:
			for command in table_init_commands:
				db_cur.execute(command)
				db_con.commit()
		except Exception as e: #Unable to create desired table
			writelog(pid, script_dir_path, scriptName, '%s.%s:\tERROR: %s' % (db_schema, table_name, e[1]), True)
			sys.exit(2)
		writelog(pid, script_dir_path, scriptName, '%s.%s:\tCREATED' % (db_schema, table_name))
	db_cur.execute('''SHOW COLUMNS FROM `%s`''' % table_name)
	raw_headers = db_cur.fetchall()
	tmp_headers = []
	for row in raw_headers:
		tmp_headers.append(row[0])
	
	db_obj = DB_handle(db_con, db_cur, table_name)	#put db parts in a class for better portability
	return db_obj
	
def fetch_cachedUntil_log(logName=cachedUntil_logName, debug=gDebug):
	None
	#TODO: write fetcher for cachedUntil.json info
	
def update_cachedUntil_log(cachedUntil_obj, logName=cachedUntil_logName, debug=gDebug):
	None
	#TODO: write updater for cachedUntil.json info
	#TODO: write locker to avoid collisions?
	
def main():
	global script_pid
	script_pid = str(os.getpid())
	
	run_all_queries = True	#run everything, unless otherwise requested
	run_pcu = False
	global gDebug
	try:
		opts, args = getopt.getopt(sys.argv[1:],"h:l", ["pcu","all",","debug"])
	except getopt.GetoptError as e:
		print str(e)
		print "unsupported argument"
		sys.exit()
	for opt, arg in opts:
		if opt == "--pcu":
			run_pcu = True
			run_all_queries = False
		elif opt == "--all":
			run_all_queries = True
			#TODO: way to keep other opts from overriding --all			
		elif opt == "--debug":
			gDebug = True
		else:
			assert False
			
	if run_all_queries:
		writelog(gLogID, script_dir_path, "run_all_queries enabled")
		run_pcu = True
		#TODO: for more feeds, add True force here
		
	##BUILD EVEAPI HANDLE##
	api_handle = eveapi.EVEAPIConnection() #TODO: try-catch to test access?
	
	##LOAD CACHE TIMER INFO##
	#TODO
	
	
	
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise