from __future__ import division
import sys, time, json, _strptime, math, os
from os import path, environ
import ConfigParser
import pypyodbc
from zkb import *

_strptime.IGNORECASE

import itertools
flatten = itertools.chain.from_iterable
current_milli_time = lambda: int(round(time.time() * 1000))

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

table_headers = {}

class Progress(object):
	def __init__ (
			self, 
			mode=default_group_mode, 
			logfile=progress_file, 
			recover=True,
			quota=zkb_scrape_limit,
			quota_period=zkb_quota_period,
			tuning_period=zkb_tuning_period,
			max_start_threads=15,
			max_threads=30):
		self.mode = mode
		self.log_base = logfile
		self.max_threads = max_threads
		self.manager = ProgressManager(quota, quota_period, tuning_period)
		self.state_lock = threading.Lock()
		self.outstanding_queries = deque()
		self.outstanding_logfile = self.mode + ".outstanding." + self.log_base
		self.running_queries = {}
		self.running_logfile = self.mode + ".running." + self.log_base
		self.failed_queries = deque()
		self.results_to_write = Queue()
		self.threads = []

		self.results_thread = threading.Thread(
			target=self.results_thread_routine,
			name="Progress results writer"
		)
		self.results_thread.daemon = True
		self.results_thread.start()

		if not (recover and self.parse_crash_log(max_start_threads)): # init object automatically
			# for testing purposes
			qb = ZKBQueryBuilder()
			for g in get_crawl_list(self.mode):
				qb.reset()
				if self.mode.upper() == "GROUP": qb.group(g)
				else: qb.ship(g)

				qb.api_only()
				print "Queuing startup thread: %s" % qb.get_query()
				self.outstanding_queries.appendleft(qb.getQueryArgs())

			self.launch_thread() # off we go!

	def wait_until_finished(self):
		iters = 0
		while True:
			done = []
			for t in list(self.threads):
				if t.is_alive(): t.join(1)
				else: done.append(t)
				iters = (iters + 1) % 30
			if self.threads and len(done) == len(self.threads):
				print "All %s outstanding threads finished." % len(done)
				break
			elif iters == 0: 
				print "%s total threads, %s done threads" % (len(done), len(self.threads))

	def results_thread_routine(self):
		data_conn, data_cur, sde_conn, sde_cur = connect_local_databases()
		mark = time.time()
		while True:
			try:
				result = self.results_to_write.get(self.manager.optimal_elapsed)
				write_kills_to_SQL(result, data_cur)
				# print "Skipping SQL write: {0} records".format(len(result))
				self.results_to_write.task_done()
			except Empty: pass
			if time.time() - mark > self.manager.tuning_period:
				mark = time.time()
				with self.state_lock:
					running = len(self.running_queries)
					outstanding = len(self.outstanding_queries)
				expected_wait = self.manager.avg_elapsed - (self.manager.quota_period * running / self.manager.quota)
				if running >= self.max_threads or self.manager.avg_wait > expected_wait > 0:
					needed = 0
				else:
					opt = self.manager.optimal_threads
					needed = int(math.ceil(opt - 0.25) - running)
					needed = min(needed, outstanding)
				print "Need {0} new threads.".format(needed)
				for _ in range(needed):
					self.launch_thread()

	def launch_thread(self, query=None):
		id = len(self.threads)
		running = len(self.running_queries)
		t = threading.Thread(
			target=self.query_thread_routine,
			kwargs={'query': query},
			name="Query thread #{0} ({1} running)".format(id, running)
		)
		t.daemon = True
		self.threads.append(t)
		t.start()

	def query_thread_routine(self, query=None):
		flow_manager = FlowManager(progress_obj=self.manager)
		me = threading.current_thread()
		while True:
			if query is None:
				with self.state_lock:
					if (not self.outstanding_queries or 
			 				1 < len(self.running_queries) > self.manager.optimal_threads + 1.25):
						if self.running_queries.has_key(me):
							del self.running_queries[me]
						flow_manager.progress.unregister()
						break
					query = self.outstanding_queries.pop()
			current_query = ZKBQuery(api_fetch_limit, query, flow_manager)
			self.running_queries[me] = current_query
			self.dump_all()
			try:
				for result in current_query:
					self.results_to_write.put(result)
					self.dump_running()
			except Exception as e:
				print "Thread %s caught exception on %s" % (me.name, current_query.get_query())
				print e
				self.failed_queries.appendleft((current_query.getQueryArgs(), str(e)))
			query = None
		self.dump_all()

	def dump_running(self):
		with self.state_lock:
			running = {}
			running['running_queries'] = [
				q.getQueryArgs() 
					for q in self.running_queries.values()
			]
			running['logfile'] = self.running_logfile

		with open(running['logfile'], 'w') as log:
			json.dump(
				obj=running,
				fp=log,
				indent=3,
				separators=(',',': ')
			)

	def build_dump_objects(self):
		outstanding = {}
		outstanding['outstanding_queries'] = list(self.outstanding_queries)
		outstanding['failed_queries'] = list(self.failed_queries)
		outstanding['mode'] = self.mode
		outstanding['logfile'] = self.outstanding_logfile

		running = {}
		running['running_queries'] = [
			q.getQueryArgs() 
				for q in self.running_queries.values()
		]
		
		running['logfile'] = self.running_logfile
		return outstanding, running
		
	def dump_all(self):
		with self.state_lock:
			os = self.build_dump_objects()
		for o in os:
			with open(o['logfile'], 'w') as log:
				json.dump(
					obj=o,
					fp=log, 
					sort_keys=True,
					indent=3,
					separators=(',',': ')
				)
		
	def parse_crash_log(self, max_threads):
		try:
			with open(self.outstanding_logfile, 'r') as log:
				outstanding = json.load(log)
			with open(self.running_logfile, 'r') as log:
				running = json.load(log)
		except Exception:
			print 'Crash file not found. Starting fresh'
			return False
		if self.mode.upper() <> outstanding['mode'].upper():
			print 'Mode mismatch. Starting fresh.'
			return False

		def backup_file(logfile):
			i = 0
			while path.exists(logfile + '.' + str(i)):
				i = i + 1
			os.rename(logfile, logfile + '.' + str(i))

		backup_file(self.outstanding_logfile)
		backup_file(self.running_logfile)

		self.mode = outstanding['mode']
		self.outstanding_queries = deque(sorted(outstanding.get('outstanding_queries', []), reverse=True))
		self.failed_queries = deque(sorted(outstanding.get('failed_queries', []), reverse=True))
		running_queries = deque(sorted(running['running_queries']))
		if not running_queries and not self.outstanding_queries:
			print "Recovery files existed but no queries were available for recovery."
			return False
		elif not running_queries:
			print "Recovery files existed but there were no running queries. Launching from queue."
			self.launch_thread()
		else:
			to_queue = len(running_queries) - min(len(running_queries), max_threads)		
			for _ in range(to_queue):
				q = running_queries.pop()
				print "Queueing recovery thread:", q
				self.outstanding_queries.append(q)
			for q in running_queries:
				print "Launching recovery thread:", q
				self.launch_thread(q)
		return True
		
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

def get_crawl_list(method):
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
			SELECT groupid
			FROM invgroups
			WHERE categoryid = 6
			'''
	else:
		raise InvalidQueryValue(method, 'Supported values are "ship" and "group".')
	
	data_conn, data_cur, sde_conn, sde_cur = connect_local_databases() #connect to SDE
	
	crawl_list = [row[0] for row in sde_cur.execute(crawl_query).fetchall()] #fetch and parse SDE call	
	return crawl_list

def archive_crawler(start_date, end_date):
	None
	
def backfill_loss_values():
	None	#use the crest_markethistory to fill any missing fit values

def build_commit_str_participants(kill_entry, base_kill_dict, isVictim = False):
	
	killID        = base_kill_dict['kill_id']
	solarSystemID = base_kill_dict['solarSystemID']
	kill_time     = base_kill_dict['kill_time']
	
	isVictim_val = 0
	if isVictim:
		isVictim_val = 1
		
	shipTypeID    = int(kill_entry['shipTypeID'])
	damage        = 0
	if isVictim:
		damage = int(kill_entry['damageTaken'])
	else:
		damage = int(kill_entry['damageDone'])
	characterID   = int(kill_entry['characterID'])
	corporationID = int(kill_entry['corporationID'])
	allianceID    = int(kill_entry['allianceID'])
	if allianceID == 0:
		allianceID = 'NULL'
	factionID     = int(kill_entry['factionID'])
	if factionID == 0:
		factionID = 'NULL'
	finalBlow = 'NULL'
	if not isVictim:
		finalBlow = int(kill_entry['finalBlow'])
	weaponTypeID = 'NULL'
	if not isVictim:
		weaponTypeID = int(kill_entry['weaponTypeID'])
	
	totalValue = base_kill_dict['totalValue']
	
	#probably better way to build this#
	return_str = '''(%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' %(\
		killID,\
		solarSystemID,\
		kill_time,\
		isVictim_val,\
		shipTypeID,\
		damage,\
		characterID,\
		corporationID,\
		allianceID,\
		factionID,\
		finalBlow,\
		weaponTypeID,\
		totalValue)
	
	return return_str

def build_commit_str_fits(item_list, base_kill_dict):
	killID      = base_kill_dict['kill_id']
	shipTypeID  = base_kill_dict['shipTypeID']
	
	typeID       = item_list['typeID']
	flag         = item_list['flag']
	qtyDropped   = item_list['qtyDropped']
	qtyDestroyed = item_list['qtyDestroyed']
	singleton    = item_list['singleton']
	
	return_str = '''(%s,%s,%s,%s,%s,%s,%s)''' % (\
		killID,\
		shipTypeID,\
		typeID,\
		flag,\
		qtyDropped,\
		qtyDestroyed,\
		singleton)
		
	return return_str

def fetch_headers(table_name, db_cur):
	global table_headers
	table_header_query = '''SHOW COLUMNS FROM `%s`''' % table_name
	table_header_list = [column[0] for column in db_cur.execute(table_header_query).fetchall()]
	table_header_str = ','.join(table_header_list)
	table_header_str = table_header_str.rstrip(',')
	table_headers[table_name] = table_header_str
	
def write_kills_to_SQL(zkb_return, db_cur, debug=False):	
	fits_list = []	#all items lost in fights
	participants_list = [] #all participants (victims and killers)
	losses_list = []	#truncated list of just victims and destroyed 
	
	for kill in zkb_return:
		base_kill_dict = {}
		
		base_kill_dict['kill_id']       = int(kill['killID'])
		base_kill_dict['solarSystemID']  = int(kill['solarSystemID'])
		base_kill_dict['kill_time']      = kill['killTime']	#convert to datetime for writing to db?
		v = float(kill.get('zkb', {}).get('totalValue', '0.0'))
		base_kill_dict['totalValue']     = v if v > 0.0 else 'NULL'	
					
		base_kill_dict['shipTypeID']     = int(kill['victim']['shipTypeID'])
		##PARSE VICTIM##
		tmp_commit_str = build_commit_str_participants(kill['victim'], base_kill_dict, True)
		participants_list.append(tmp_commit_str)
		
		for participant in kill['attackers']:	#walk through participants to get kill stats
			tmp_commit_str = build_commit_str_participants(participant, base_kill_dict, False)
			participants_list.append(tmp_commit_str)
		
		for item_list in kill['items']:
			tmp_commit_str = build_commit_str_fits(item_list, base_kill_dict)
			fits_list.append(tmp_commit_str)
		
		v = int(kill['victim'].get('allianceID', 0))
		victim_allianceID = v if v <> 0 else 'NULL'
		v = int(kill['victim'].get('factionID', 0))			
		victim_factionID = v if v <> 0 else 'NULL'
			
		losses_str = '''(%s,%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s)'''%(\
			base_kill_dict['kill_id'],\
			base_kill_dict['solarSystemID'],\
			base_kill_dict['kill_time'],\
			base_kill_dict['shipTypeID'],\
			kill['victim']['damageTaken'],\
			kill['victim']['characterID'],\
			kill['victim']['corporationID'],\
			victim_allianceID,\
			victim_factionID,\
			base_kill_dict['totalValue'],
			len(kill['attackers']))
			
		losses_list.append(losses_str)

	def write_to_table(table_name, data_list):
		if table_name not in table_headers:
			fetch_headers(table_name, db_cur)
		
		commit_preface = 'INSERT INTO {table} ({headers}) VALUES'.format(table=table_name, headers=table_headers[table_name])
		commit_list = ', '.join(data_list)
		
		#DUPLICATE OVERWRITE#
		duplicate_preface = 'ON DUPLICATE KEY UPDATE' #TODO: figure out a dynamic way to use duplicate key lookup
		duplicate_list = ', '.join('{0}={0}'.format(header) for header in table_headers[table_name].split(','))
		
		to_commit_str = ' '.join([commit_preface, commit_list, duplicate_preface, duplicate_list])
		#-------------------#
		if debug: print to_commit_str
		db_cur.execute(to_commit_str).commit()

####WRITE PARTICIPANTS TABLE####
	write_to_table(zkb_participants, participants_list)	
	
####WRITE FITS TABLE####
	write_to_table(zkb_fits, fits_list)	

####WRITE LOSSES TABLE####
	write_to_table(zkb_trunc_stats, losses_list)

def main():
	_validate_connection()
	progress = Progress(mode='ship')
	progress.wait_until_finished()

if __name__ == "__main__":
	main()