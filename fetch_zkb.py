import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path, environ
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime, timedelta
import zkb

import itertools
flatten = itertools.chain.from_iterable

conf = ConfigParser.ConfigParser()
conf.read(['init.ini','init_local.ini'])

####DB STUFF####
db_host   = conf.get('GLOBALS','db_host')
db_user   = conf.get('GLOBALS','db_user')
db_pw     = environ.get('MYSQL_%s_PW' % db_user.upper(), '')
db_pw     = db_pw if db_pw else conf.get('GLOBALS','db_pw')
db_port   = int(conf.get('GLOBALS','db_port'))
db_schema = conf.get('GLOBALS','db_schema')
db_driver = conf.get('GLOBALS','db_driver')
sde_schema  = conf.get('GLOBALS','sde_schema')

####TABLES####
zkb_participants = conf.get('TABLES','zkb_participants')
zkb_fits         = conf.get('TABLES','zkb_fits')
zkb_trunc_stats  = conf.get('TABLES','zkb_trunc_stats')

def connect_local_databases(*args):
	global db_driver, db_host, db_port, db_user, db_pw, db_schema, sde_schema
	schemata = args if args else [db_schema, sde_schema]
	connections = [
		pypyodbc.connect(
			'DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DATABASE=%s' 
			% (db_driver,db_host,db_port,db_user,db_pw,schema)
			)
		for schema in schemata
		]
	return flatten([conn, conn.cursor()] for conn in connections)
	
def _validate_connection(
		tables=[zkb_participants, zkb_fits, zkb_trunc_stats],
		schema=db_schema,
		debug=False
		):
	db_conn, db_cur = connect_local_databases(schema)

	def _initSQL(table_name):	
		db_cur.execute('''SHOW TABLES LIKE \'%s\'''' % table_name)
		table_exists = len(db_cur.fetchall())
		if table_exists:	#if len != 0, table is already initialized
			print '%s.%s:\tGOOD' % (schema,table_name)
			return
		else:
			table_init = open(path.relpath('sql/%s.mysql' % table_name)).read()
			table_init_commands = table_init.split(';')
			try:
				for command in table_init_commands:
					if debug: print command
					db_cur.execute(command).commit()
			except Exception as e:
				sys.stdout.write('%s.%s:\tERROR\t%s\n' % (schema,table_name,e[1]))
				sys.exit(2)
			sys.stdout.write('%s.%s:\tCREATED\n' % (schema,table_name))
			return
	
	for table in tables:
		_initSQL(table)
	db_conn.close()	

def main():
	_validate_connection()
	#TODO: test if zkb API is up

if __name__ == "__main__":
	main()