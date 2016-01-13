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

script_dir_path = "%s/logs/" % os.path.dirname(os.path.realpath(__file__))

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
		
	
def main():
	run_all_queries = True	#run everything, unless otherwise requested
	run_pcu = False
	
	global gDebug
	try:
		opts, args = getopt.getopt(sys.argv[1:],'h:l', ['pcu','debug'])
	except getopt.GetoptError as e:
		print str(e)
		print 'unsupported argument'
		sys.exit()
	for opt, arg in opts:
		if opt == '--pcu':
			run_pcu = True
			run_all_queries = False
		elif opt == '--debug':
			gDebug = True
		else:
			assert False
			
	if run_all_queries:
		writelog(gLogID, script_dir_path, "run_all_queries enabled")
		run_pcu = True
		#TODO: for more feeds, add True force here
		
	
	
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise