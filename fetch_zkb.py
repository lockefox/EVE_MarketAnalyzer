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
zkb_fits         = conf.get('TABLES','zkb_fits')
zkb_trunc_stats  = conf.get('TABLES','zkb_trunc_stats')
zkb_participants = conf.get('TABLES','zkb_participants')

####GLOBALS####
progress_file = conf.get('ZKB','progress_file')
api_fetch_limit = conf.get('ZKB','api_fetch_limit')
zkb_base_query = conf.get('ZKB','base_query')
default_group_mode = conf.get('ZKB','group_routine')

progress_object = {}

class Progress(object):
	__initialized = False
	__fresh_run = False
	def __init__ (self, mode = default_group_mode):
		self.latest_query = ''
		self.killIDs = []
		self.groups_completed = []
		self.groups_remaining = []
		self.mode = mode
		self.parse_crash_log()	#init object automatically
		
		if __fresh_run:
			None #init from base if parse_crash_log returns nothing
			
	def dump_object(self):
		dump_object = {}
		
		dump_object['killIDs'] = self.killIDs
		dump_object['groups_completed'] = self.groups_completed
		dump_object['groups_remaining'] = self.groups_remaining
		dump_object['latest_query'] = self.latest_query
		dump_object['mode'] = self.mode
		
		return dump_object
		
	def dump_crash_log (self, json_file = progress_file):
		file = open(json_file,'w')
		file.write(json.dumps(self.dump_object(), sort_keys=True, indent=3, separators=(',',': '))
		file.close()
		
	def parse_crash_log(self, json_file = progress_file):
		try:
			file = open(json_file,'r')
		except Exception as e:
			print 'crash file not found'
			__fresh_run = True
			return
		dump_object = json.load(file)
		file.close()
		
		self.latest_query = dump_object['mode']
		
		if self.latest_query != eq default_group_mode:
			print 'Modes don\'t match.  Starting fresh'
			__fresh_run = True
			return
		
		self.killIDs = dump_object['killIDs']
		self.groups_completed = dump_object['groups_completed']
		self.groups_remaining = dump_object['groups_remaining']
		self.latest_query = dump_object['latest_query']
		
		
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

def get_crawl_list (method):
	crawl_list = []
	crawl_query = ''
	if method.upper() == 'SHIP':
		crawl_query = '''
			SELECT conv.typeid
			FROM invtypes conv
			JOIN invgroups grp ON (conv.groupID = grp.groupID)
			WHERE categoryid = 6'''
	elif method.upper() == 'GROUP':
		crawl_query = '''
			SELECT grp.groupid
			FROM invtypes conv
			JOIN invgroups grp ON (conv.groupID = grp.groupID)
			WHERE categoryid = 6
			GROUP BY grp.groupid'''
	else:
		print 'Unsupported fetch method: %s' % method.upper()
		sys.exit(2)
	
	data_conn, data_cur, sde_conn, sde_cur = connect_local_databases() #connect to SDE
	
	crawl_list = [row[0] for row in sde_cur.execute(crawl_query).fetchall()] #fetch and parse SDE call	
	return crawl_list

def archive_crawler(start_date, end_date):
	None
	
def backfill_loss_values():
	None	#use the crest_markethistory to fill any missing fit values

def _dump_progress(latest_query, kill_id_list):
	global progress_object
	
	progress_object['queries'].append(latest_query)
	for kill_id in kill_id_list:
		progress_object['kill_ids'].append(kill_id)	#probably going to make list huge
		
	progress_object['last_query'] = latest_query
	
	dumpfile = open(progress_file, 'w')
	dumpfile.write(json.dumps(progress_object))
	dumpfile.close()
	
def _load_progress():
	global progress_object		
	try:
		progress_file_obj = open(progress_file)
	except Exception as e:
		print "no progress file found"
		#need progress_file_obj.close()?
		return
	progress_object = json.load(progress_file_obj)
	progress_file_obj.close()
	
def main():
	_validate_connection()
	#TODO: test if zkb API is up
	_load_progress()
	crawl_list = get_crawl_list('GROUP')
	
	Query_bulk = zkb.Query(api_fetch_limit)
	if 'last_query' in progress_object:
		#overwrite query object with forced raw args
		query_args = progress_object['last_query'].replace(zkb_base_query,'')
		Query_bulk = zkb.Query(api_fetch_limit, progress_object['last_query'])
	

if __name__ == "__main__":
	main()