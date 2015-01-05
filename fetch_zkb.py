from __future__ import division
import sys, time, json, _strptime
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
	def __init__ (self, mode=default_group_mode, logfile=progress_file):
		self.mode = mode
		self.log_base = logfile
		self.manager = ProgressManager()
		self.state_lock = threading.Lock()
		self.outstanding_queries = deque()
		self.running_queries = {}
		self.results_to_write = Queue()
		self.threads = []

		self.results_thread = threading.Thread(target=self.results_thread_routine)
		self.results_thread.daemon = True
		self.results_thread.start()

		if not self.parse_crash_log(): # init object automatically
			# get the crawl list etc
			self.launch_thread() # off we go!

	def wait_until_finished(self):
		for t in self.threads:
			t.join()

	def results_thread_routine(self):
		data_conn, data_cur, sde_conn, sde_cur = connect_local_databases()
		mark = time.time()
		while True:
			result = self.results_to_write.get()
			write_kills_to_SQL(result, db_cur)
			self.results_to_write.task_done()
			if time.time() - mark > self.manager.tuning_period:
				mark = time.time()
				opt = self.manager.optimal_threads
				if self.outstanding_queries:
					while opt > len(self.threads) + 0.25:
						print "Launching new worker thread."
						self.launch_thread()

	def launch_thread(self, query=None):
		t = threading.Thread(
			target=self.query_thread_routine,
			kwargs={'query': query}
		)
		t.daemon = True
		self.threads.append(t)
		t.start()

	def query_thread_routine(self, query=None):
		flow_manager = FlowManager(progress_obj=self.manager)
		me = threading.current_thread()
		while True:
			with self.state_lock:
				if query is None:
					if not self.outstanding_queries:
						if self.running_queries.has_key(me):
							del self.running_queries[me]
						return
					query = self.outstanding_queries.pop()
				current_query = ZKBQuery(api_fetch_limit, query, flow_manager)
				self.running_queries[me] = current_query
			self.dump_all()
			for result in current_query:
				self.results_to_write.put(result)
				self.dump_running()
			query = None

	def dump_running(self):
		with self.state_lock():
			running = {}
			running['running_queries'] = [
				q.getQueryArgs() 
					for q in self.running_queries.values()
			]
			running['logfile'] = "running." + self.log_base
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
		outstanding['mode'] = self.mode
		outstanding['logfile'] = "outstanding." + self.log_base

		running = {}
		running['running_queries'] = [
			q.getQueryArgs() 
				for q in self.running_queries.values()
		]
		running['logfile'] = "running." + self.log_base
		return outstanding, running
		
	def dump_all(self):
		with self.state_lock:
			for o in self.build_dump_objects():
				with open(o['logfile'], 'w') as log:
					json.dump(
						obj=o,
						fp=log, 
						sort_keys=True,
						indent=3,
						separators=(',',': ')
					)
		
	def parse_crash_log(self):
		try:
			with open("outstanding." + self.log_base, 'r') as log:
				outstanding = json.load(log)
			with open("running." + self.log_base, 'r') as log:
				running = json.load(log)
		except Exception:
			print 'Crash file not found.  Starting fresh'
			return False
			
		self.mode = outstanding['mode']
		self.outstanding_queries = deque(outstanding.get('outstanding_queries', []))
		for q in running['running_queries']:
			self.launch_thread(q)
		
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
		print 'Unsupported fetch method: %s' % method.upper()
		sys.exit(2)
	
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
		base_kill_dict['totalValue']     = 'NULL'
		
		if 'zkb' in kill:
			if 'totalValue' in kill['zkb']:
				if int(float(kill['zkb']['totalValue'])) != 0:
					base_kill_dict['totalValue'] = float(kill['zkb']['totalValue']) 
					
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
		
		victim_allianceID = 'NULL'
		if int(kill['victim']['allianceID']) != 0:
			victim_allianceID = int(kill['victim']['allianceID'])
			
		victim_factionID = 'NULL'
		if int(kill['victim']['factionID']) != 0:
			victim_factionID = int(kill['victim']['factionID'])
			
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
	
####WRITE PARTICIPANTS TABLE####
	if zkb_participants not in table_headers:
		fetch_headers(zkb_participants, db_cur)
	
	participants_commit_str = '''INSERT INTO %s (%s) VALUES''' % (zkb_participants,table_headers[zkb_participants])
	
	for participant_str in participants_list:
		participants_commit_str = '%s %s,' % (participants_commit_str,participant_str)
	
	participants_commit_str = participants_commit_str.rstrip(',')
		#DUPLICATE OVERWRITE#
	participants_duplicate = '''ON DUPLICATE KEY UPDATE''' #TODO: figure out a dynamic way to use duplicate key lookup
	for header in table_headers[zkb_participants].split(','):
		participants_duplicate = '%s %s=%s,' % (participants_duplicate, header, header)
	participants_commit_str = '%s %s' % (participants_commit_str, participants_duplicate.rstrip(','))
		#-------------------#
	if debug: print participants_commit_str
	db_cur.execute(participants_commit_str).commit()	
	
####WRITE FITS TABLE####
	if zkb_fits not in table_headers:
		fetch_headers(zkb_fits, db_cur)
		
	fits_commit_str = '''INSERT INTO %s (%s) VALUES''' % (zkb_fits, table_headers[zkb_fits])
	
	for fits_str in fits_list:
		fits_commit_str = '%s %s,' % (fits_commit_str, fits_str)
		
	fits_commit_str = fits_commit_str.rstrip(',')
		#DUPLICATE OVERWRITE#
	fits_duplicate = '''ON DUPLICATE KEY UPDATE''' #TODO: figure out a dynamic way to use duplicate key lookup
	for header in table_headers[zkb_fits].split(','):
		fits_duplicate = '%s %s=%s,' % (fits_duplicate, header, header)
	fits_commit_str = '%s %s' % (fits_commit_str, fits_duplicate.rstrip(','))
		#-------------------#
	if debug: print fits_commit_str
	db_cur.execute(fits_commit_str).commit()

####WRITE LOSSES TABLE####
	if zkb_trunc_stats not in table_headers:
		fetch_headers(zkb_trunc_stats, db_cur)
		
	losses_commit_str = '''INSERT INTO %s (%s) VALUES''' % (zkb_trunc_stats, table_headers[zkb_trunc_stats])
	
	for losses_str in losses_list:
		losses_commit_str = '%s %s,' % (losses_commit_str, losses_str)
		
	losses_commit_str = losses_commit_str.rstrip(',')
		#DUPLICATE OVERWRITE#
	losses_duplicate = '''ON DUPLICATE KEY UPDATE''' #TODO: figure out a dynamic way to use duplicate key lookup
	for header in table_headers[zkb_trunc_stats].split(','):
		losses_duplicate = '%s %s=%s,' % (losses_duplicate, header, header)
	losses_commit_str = '%s %s' % (losses_commit_str, losses_duplicate.rstrip(','))
		#-------------------#
	if debug: print losses_commit_str
	db_cur.execute(losses_commit_str).commit()
	
def main():
	_validate_connection()
	#TODO: test if zkb API is up
	print 'Building crash object'
	progress = Progress()
	progress.wait_until_finished()
	
	print 'Fetching zkb data'
	####FETCH LIVE KILL DATA####
	for group in progress.groups_remaining:
		QueryObj = zkb.ZKBQuery(api_fetch_limit)
		
		##TODO: add multi-group scraping.  Joined group_list should work in setup below
		if   progress.mode == 'SHIP': QueryObj.shipID(group)
		elif progress.mode == 'GROUP': QueryObj.groupID(group)
		else: 
			print 'Unsupported fetch method: %s' % method.upper()
			sys.exit(2)
		QueryObj.api_only()
	
		if progress.latestKillID != 0:	#recover progress
			QueryObj.beforeKillID(progress.latestKillID)
			
		print 'Fetching %s' % QueryObj
		for kill_list in QueryObj:
			print '\t%s' % QueryObj
			write_kills_to_SQL(kill_list, data_cur, False)
			progress.update_query(str(QueryObj))
			progress.dump_crash_log()
		progress.group_complete(group)	#TODO: will need to parse out CSV to list?
		progress.latestKillID = 0
		progress.dump_crash_log()
		#sys.exit(1)
if __name__ == "__main__":
	main()