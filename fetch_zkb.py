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
		
		if self.__fresh_run:
			self.groups_remaining = get_crawl_list(self.mode)
					
	def build_dump_object(self):
		dump_object = {}
		
		dump_object['killIDs'] = self.killIDs
		dump_object['groups_completed'] = self.groups_completed
		dump_object['groups_remaining'] = self.groups_remaining
		dump_object['latest_query'] = self.latest_query
		dump_object['mode'] = self.mode
		
		return dump_object
		
	def dump_crash_log (self, json_file = progress_file):
		file = open(json_file,'w')
		file.write(json.dumps(self.build_dump_object(), sort_keys=True, indent=3, separators=(',',': ')))
		file.close()
		
	def parse_crash_log(self, json_file = progress_file):
		try:
			file = open(json_file,'r')
		except Exception as e:
			print 'Crash file not found.  Starting fresh'
			self.__fresh_run = True
			return
		dump_object = json.load(file)
		file.close()
		
		if self.mode != dump_object['mode']:
			print 'Modes don\'t match.  Starting fresh'
			self.__fresh_run = True
			return
			
		self.mode = dump_object['mode']
		self.killIDs = dump_object['killIDs']
		self.groups_completed = dump_object['groups_completed']
		self.groups_remaining = dump_object['groups_remaining']
		self.latest_query = dump_object['latest_query']

	def addKillID(self,newkillID):
		self.killIDs.append(int(newkillID))
	
	def update_query(self,query_str):
		self.latest_query = query_str
		
	def group_complete(self, completed_group_id):
		print 'group completed: %s' % completed_group_id
		self.groups_remaining.remove(completed_group_id)
		self.groups_completed.append(completed_group_id)
		
	##TODO: __iter__ to have progress walk the query and manage tracking	
	
	def __str__ (self):
		return self.latest_query
		
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
		shipTypeID,\
		isVictim_val,\
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
	
def write_kills_to_SQL(zkb_return):
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
	print 'fits'
	print fits_list
	print 'participants'
	print participants_list
	print 'losses'
	print losses_list
	sys.exit(1)
	
def main():
	_validate_connection()
	#TODO: test if zkb API is up
	print 'Building crash object'
	ProgressObj = Progress()
	
	print 'Fetching zkb data'
	####FETCH LIVE KILL DATA####
	for group in ProgressObj.groups_remaining:
		QueryObj = zkb.Query(api_fetch_limit)
		
		##TODO: add multi-group scraping.  Joined group_list should work in setup below
		if   ProgressObj.mode == 'SHIP': QueryObj.shipID(group)
		elif ProgressObj.mode == 'GROUP': QueryObj.groupID(group)
		else: 
			print 'Unsupported fetch method: %s' % method.upper()
			sys.exit(2)
		QueryObj.api_only
		print 'Fetching %s' % QueryObj
		for kill_list in QueryObj:
			write_kills_to_SQL(kill_list)
			#TODO: write killid list to ProgressObj
			
		ProgressObj.group_complete(group)	#TODO: will need to parse out CSV to list?
		
if __name__ == "__main__":
	main()