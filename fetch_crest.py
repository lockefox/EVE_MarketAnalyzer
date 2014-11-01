#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path, environ
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime as dt, timedelta as td
import threading

import itertools
flatten = itertools.chain.from_iterable
strptime = dt.strptime


conf = ConfigParser.ConfigParser()
conf.read(['init.ini','init_local.ini'])
####GLOBALS####
crest_path = conf.get('CREST','default_path')
crest_test_path = conf.get('CREST','test_path')
user_agent = conf.get('GLOBALS','user_agent')
retry_limit = int(conf.get('GLOBALS','default_retries'))
sleep_timer = int(conf.get('GLOBALS','default_sleep'))
crash_filename_base = conf.get('CREST','progress_file_base')
tick_delay = td(seconds=10)
tick_delay_dbg = td(seconds=5)

####DB STUFF####
db_host   = conf.get('GLOBALS','db_host')
db_user   = conf.get('GLOBALS','db_user')
db_pw     = environ.get('MYSQL_%s_PW' % db_user.upper(), '')
db_pw     = db_pw if db_pw else conf.get('GLOBALS','db_pw')
db_port   = int(conf.get('GLOBALS','db_port'))
db_schema = conf.get('GLOBALS','db_schema')
db_driver = conf.get('GLOBALS','db_driver')
sde_schema  = conf.get('GLOBALS','sde_schema')

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

####TABLES####
crest_pricehistory  = conf.get('TABLES','crest_pricehistory')
crest_industryindex = conf.get('TABLES','crest_industryindex')
crest_serverprices  = conf.get('TABLES','crest_serverprices')

def thread_print(msg):
	sys.stdout.write("%s\n" % msg)
	sys.stdout.flush()

####CONST STUFF####
region_list = {
	'10000001':'Derelik',
	'10000002':'The Forge',
	'10000003':'Vale of the Silent',
	'10000005':'Detorid',
	'10000006':'Wicked Creek',
	'10000007':'Cache',
	'10000008':'Scalding Pass',
	'10000009':'Insmother',
	'10000010':'Tribute',
	'10000011':'Great Wildlands',
	'10000012':'Curse',
	'10000013':'Malpais',
	'10000014':'Catch',
	'10000015':'Venal',
	'10000016':'Lonetrek',
	'10000018':'The Spire',
	'10000020':'Tash-Murkon',
	'10000021':'Outer Passage',
	'10000022':'Stain',
	'10000023':'Pure Blind',
	'10000025':'Immensea',
	'10000027':'Etherium Reach',
	'10000028':'Molden Heath',
	'10000029':'Geminate',
	'10000030':'Heimatar',
	'10000031':'Impass',
	'10000032':'Sinq Laison',
	'10000033':'The Citadel',
	'10000034':'The Kalevala Expanse',
	'10000035':'Deklein',
	'10000036':'Devoid',
	'10000037':'Everyshore',
	'10000038':'The Bleak Lands',
	'10000039':'Esoteria',
	'10000040':'Oasa',
	'10000041':'Syndicate',
	'10000042':'Metropolis',
	'10000043':'Domain',
	'10000044':'Solitude',
	'10000045':'Tenal',
	'10000046':'Fade',
	'10000047':'Providence',
	'10000048':'Placid',
	'10000049':'Khanid',
	'10000050':'Querious',
	'10000051':'Cloud Ring',
	'10000052':'Kador',
	'10000053':'Cobalt Edge',
	'10000054':'Aridia',
	'10000055':'Branch',
	'10000056':'Feythabolis',
	'10000057':'Outer Ring',
	'10000058':'Fountain',
	'10000059':'Paragon Soul',
	'10000060':'Delve',
	'10000061':'Tenerifis',
	'10000062':'Omist',
	'10000063':'Period Basis',
	'10000064':'Essence',
	'10000065':'Kor-Azor',
	'10000066':'Perrigen Falls',
	'10000067':'Genesis',
	'10000068':'Verge Vendor',
	'10000069':'Black Rise'
	}

trunc_region_list = {
	'10000002':'The Forge',
	'10000043':'Domain',
	'10000032':'Sinq Laison',
	'10000042':'Metropolis',
	}
	
def _validate_connection(
		tables=[crest_pricehistory, crest_industryindex, crest_serverprices],
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
				sys.stdout.write('.%s:\tERROR\t%s\n' % (table_name,e[1]))
				sys.exit(2)
			sys.stdout.write('.%s:\tCREATED\n' % table_name)
			return
	
	for table in tables:
		_initSQL(table)
	db_conn.close()

def fetch_markethistory(regions={}, debug=False, testserver=False):
	if not regions:	raise ValueError("Argument region_list may not be empty.")

	start = dt.now()
	last = dt.now()

	data_conn, data_cur, sde_conn, sde_cur = connect_local_databases()
	thread_print( "FETCHING CREST/MARKET_HISTORY on thread %s" % threading.current_thread().name )

	 #remove typeid NOT IN eventualy
	items_query = '''
		SELECT typeid
		FROM invtypes conv
		JOIN invgroups grp ON (conv.groupID = grp.groupID)
		WHERE marketgroupid IS NOT NULL
		AND conv.published = 1
		AND grp.categoryid NOT IN (9,16,350001,2)
		AND grp.groupid NOT IN (30,659,485,485,873,883)
		ORDER BY typeid
		'''

	item_list = [row[0] for row in sde_cur.execute(items_query).fetchall()]
	
	price_history_query = 'SHOW COLUMNS FROM `%s`' % crest_pricehistory
	price_history_headers = [column[0] for column in data_cur.execute(price_history_query).fetchall()]
	
	def print_progress():
		delay = tick_delay_dbg if debug else tick_delay
		now = dt.now()
		if (now - last) > delay:
			timed_msg = "{}: {}/{} ({:.1f} m elapsed / {:.1f} m remaining)"
			elapsed = (now - start).total_seconds() / 60
			remaining = (len(item_list) - count) * elapsed / count if count else float('NaN')
			thread_print( timed_msg.format(regionName, count, len(item_list), elapsed, remaining) )
			return now
		else: 
			return last 

	for regionID, regionName in regions.iteritems():
		crash_JSON = recover_on_restart(regionID)
		print regionName
		
		if len(crash_JSON['market_history'][regionID]) >= len(item_list):
			thread_print( '\tRegion Complete' )
			continue

		for count,itemID in enumerate(item_list):
			last = print_progress()
			query = 'market/%s/types/%s/history/' % (regionID,itemID)
			if itemID in crash_JSON['market_history'][regionID]:
				if debug: thread_print( '%s:\tskip' % query )
				continue #already processed data
			
			price_JSON = fetchURL_CREST(query, testserver, debug=False)
			
			if len(price_JSON['items']) == 0: 
				write_progress('market_history',regionID,itemID,crash_JSON)
				if debug: thread_print( '%s:\tEMPTY' % query )
				continue
			data_to_write = []
			for entry in price_JSON['items']:
				line_to_write = []
				line_to_write.append(_date_convert(entry['date']))
				line_to_write.append(itemID)
				line_to_write.append(int(regionID))
				line_to_write.append(entry['orderCount'])
				line_to_write.append(entry['volume'])
				line_to_write.append(float(entry['lowPrice']))
				line_to_write.append(float(entry['highPrice']))
				line_to_write.append(float(entry['avgPrice']))
				data_to_write.append(line_to_write)
			
			writeSQL(data_cur,crest_pricehistory,price_history_headers,data_to_write)
			write_progress('market_history',regionID,itemID,crash_JSON)

def writeSQL(db_cur, table, headers_list, data_list, hard_overwrite=True, debug=False):
	insert_statement = '''INSERT INTO %s (%s) VALUES''' % (table, ','.join(headers_list))
	if debug: thread_print( insert_statement )

	for entry in data_list:
		value_string = ''
		for value in entry:
			if isinstance(value, (int,long,float)): #if number, add value
				value_string = '%s,%s' % ( value_string, value)
			elif value == None:
				value_string = '%s,NULL' % ( value_string)
			else:
				value = value.replace('\'', '\\\'') #sanitize apostrophies
				value_string = '%s,\'%s\'' % ( value_string, value)
		value_string = value_string[1:]
		if debug: thread_print( value_string )
		insert_statement = '%s (%s),' % (insert_statement, value_string)
	
	insert_statement = insert_statement[:-1]	#pop off trailing ','
	if hard_overwrite:
		duplicate_str = '''ON DUPLICATE KEY UPDATE '''
		for header in headers_list:
			duplicate_str = "%s %s=%s," % (duplicate_str, header, header)
		
		insert_statement = "%s %s" % (insert_statement, duplicate_str)
		insert_statement = insert_statement[:-1]	#pop off trailing ','
	if debug: thread_print( insert_statement )
	db_cur.execute(insert_statement).commit()
	
def fetchURL_CREST(query, testserver=False, debug=False):
	#Returns parsed JSON of CREST query
	real_query = ''
	if testserver: real_query = '%s%s' % (crest_test_path, query)
	else: real_query = '%s%s' % (crest_path, query)
	
	if debug: thread_print( real_query )
	
	request = urllib2.Request(real_query)
	request.add_header('Accept-Encoding','gzip')
	request.add_header('User-Agent',user_agent)
	
	headers = {}
	for tries in range (0,retry_limit):
		time.sleep(sleep_timer*tries)
		try:
			opener = urllib2.build_opener()
			raw_response = opener.open(request)
			headers = raw_response.headers
			response = raw_response.read()
		except urllib2.HTTPError as e:
			thread_print( 'HTTPError:%s %s' % (e,real_query) )
			continue
		except urllib2.URLError as e:
			thread_print( 'URLError:%s %s' % (e,real_query) )
			continue
		except socket.error as e:
			thread_print( 'Socket Error:%s %s' % (e,real_query) )
			continue
		
		do_gzip = headers.get('Content-Encoding','') == 'gzip'
				
		if do_gzip:
			try:
				buf = StringIO.StringIO(response)
				zipper = gzip.GzipFile(fileobj=buf)
				return_result = json.load(zipper)
			except ValueError as e:
				thread_print( "Empty response: Retry %s" % real_query )
				continue
			except IOError as e:
				thread_print( "gzip unreadable: Retry %s" % real_query )
				continue
			else:
				break
		else:
			return_result = json.loads(response)
			break
	else:
		thread_print( headers )
		sys.exit(2)
	
	return return_result
	
def _date_convert(date_str):
	new_time = strptime(date_str,'%Y-%m-%dT%H:%M:%S')
	return new_time.strftime('%Y-%m-%d')

def recover_on_restart(region_id):
	crash_filename = region_id + "_" + crash_filename_base
	crash_JSON = {}
	try:
		with open(crash_filename,'r') as f:
			crash_JSON = json.load(f)
		if ((not 'market_history' in crash_JSON) or 
			(not region_id in crash_JSON['market_history'])
			):
			raise Exception("Corrupted recovery file.")
		thread_print( 'Loaded progress file %s' % crash_filename )
	except Exception as e:
		thread_print( 'No recovery file found, starting fresh' )
		crash_JSON['market_history'] = {}
		crash_JSON['market_history'][region_id] = {}
	crash_JSON['filename'] = crash_filename
	return crash_JSON

def write_progress(subtable_name, key1, key2, crash_JSON):
	if subtable_name not in crash_JSON:
		crash_JSON[subtable_name]={}
	if key1 not in crash_JSON[subtable_name]:
		crash_JSON[subtable_name][key1]={}
	crash_JSON[subtable_name][key1][key2]=1
	
	with open(crash_JSON['filename'],'w') as crash_file:
		json.dump(crash_JSON, crash_file)

def launch_region_threads(regions={}):
	region_threads = []
	for region_id, region_name in regions.iteritems():
		kwargs = {
			'regions': {region_id: region_name},
			'debug': False,
			'testserver': False
			}
		new_thread = threading.Thread(
			name=region_name, 
			kwargs=kwargs, 
			target=fetch_markethistory
			)
		new_thread.daemon = True
		region_threads.append(new_thread)
		new_thread.start()
	return region_threads		

def wait_region_threads(threads=[]):
	while True:
		done = 0
		for t in threads:
			if t.is_alive(): t.join(1)
			else: 
				thread_print( t.name + " has finished." )
				done = done + 1
		if done == len(threads):
			break

def _optimize_database():
	thread_print( "Optimizing database %s..." % crest_pricehistory )
	data_conn, data_cur = connect_local_databases(db_schema)
	data_cur.execute('''OPTIMIZE TABLE `%s`''' % crest_pricehistory).commit()
	data_conn.close()

def main():
	_validate_connection()
	region_threads = launch_region_threads(trunc_region_list)
	wait_region_threads(region_threads)
	_optimize_database()

if __name__ == "__main__":
	main()
