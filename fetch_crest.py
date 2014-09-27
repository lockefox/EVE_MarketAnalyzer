#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime

conf = ConfigParser.ConfigParser()
conf.read(['init.ini','init_local.ini'])

####DB STUFF####
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

####TABLES####
crest_pricehistory  = conf.get('TABLES','crest_pricehistory')
crest_industryindex = conf.get('TABLES','crest_industryindex')
crest_serverprices  = conf.get('TABLES','crest_serverprices')

def _validate_connection():
	global data_conn, data_cur, sde_conn, sde_cur
	
	sys.stdout.write('%s' % (db_schema))	#sys.stdout.write to avoid \n of print
	_initSQL(crest_pricehistory, data_cur, data_conn)
	
	sys.stdout.write('%s' % (db_schema))	#sys.stdout.write to avoid \n of print
	_initSQL(crest_industryindex, data_cur, data_conn)
	
	sys.stdout.write('%s' % (db_schema))	#sys.stdout.write to avoid \n of print
	_initSQL(crest_serverprices, data_cur, data_conn)

def _initSQL(table_name, db_cur, db_conn, debug=False):	
	db_cur.execute('''SHOW TABLES LIKE \'%s\'''' % table_name)
	table_exists = len(db_cur.fetchall())
	if table_exists:	#if len != 0, table is already initialized
		sys.stdout.write('.%s:\tGOOD\n'%table_name)
		return
	else:
		table_init = open(path.relpath('sql/%s.sql' % table_name)).read()
		table_init_commands = table_init.split(';')
		try:
			for command in table_init_commands:
				if debug:
					print command
				db_cur.execute(command)
				db_conn.commit()
		except Exception as e:
			sys.stdout.write('.%s:\tERROR\t%s\n' % (table_name,e[1]))
			sys.exit(2)
		sys.stdout.write('.%s:\tCREATED\n' % table_name)
		return
		
def main():
	_validate_connection()
		
if __name__ == "__main__":
	main()