#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket,glob
from os import path, environ
import urllib2
import httplib
import pypyodbc
from datetime import datetime, timedelta
import _strptime # because threading
import threading
import requests
import random

from ema_config import *
thread_exit_flag = False

def thread_print(msg):
	sys.stdout.write("%s\n" % msg)
	sys.stdout.flush()

def print_progress(timed_msg, last, start, count, total, debug=False):
	'''timed_msg parameter may optionally contain format specifiers referencing these variables:
	finished: number of items finished
	total: total number of items
	elapsed: time elapsed
	remaining: estimated time remaining'''
	delay = tick_delay_dbg if debug else tick_delay
	now = datetime.now()
	if (now - last) > delay:
		elapsed = (now - start).total_seconds() / 60
		remaining = (total - count) * elapsed / count if count else float('NaN')
		thread_print( timed_msg.format(
			finished=count,
			total=total,
			elapsed=elapsed,
			remaining=remaining
			) )
		return now
	else:
		return last


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
			table_init = open(path.relpath('SQL/%s.mysql' % table_name)).read()
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

def fetch_markethistory(regions={}, thread_id=(0,1), debug=False, testserver=False):
	if not regions:	raise ValueError("Argument region_list may not be empty.")
	me, parts = thread_id
	start = datetime.now()
	last = datetime.now()

	data_conn, data_cur, sde_conn, sde_cur = connect_local_databases()
	thread_print( "FETCHING CREST/MARKET_HISTORY on thread %s" % threading.current_thread().name )

	 #remove typeid NOT IN eventualy
	items_query = '''
		SELECT typeid
		FROM invTypes conv
		JOIN invGroups grp ON (conv.groupID = grp.groupID)
		WHERE marketgroupid IS NOT NULL
		AND conv.published = 1
		AND grp.categoryid NOT IN (9,16,350001,2)
		AND grp.groupid NOT IN (30,659,485,485,873,883)
		ORDER BY typeid
		'''

	item_list = [row[0] for row in sde_cur.execute(items_query).fetchall()]

	price_history_query = '''SHOW COLUMNS FROM `%s`''' % crest_pricehistory
	price_history_headers = [column[0] for column in data_cur.execute(price_history_query).fetchall()]

	nitems = len(item_list)
	tail = nitems % parts
	tail = tail if tail <= me else 0
	my_count = nitems//parts + tail
	def print_progress_thread():
		timed_msg = fmt_name + " {finished}/{total} ({elapsed:.1f} m elapsed / {remaining:.1f} m remaining)"
		return print_progress(timed_msg, last, start, i_finished, my_count, debug)

	for regionID, regionName in regions.iteritems():
		fmt_name = region_name_format.format( regionName + ":", me )
		crash_JSON = recover_on_restart(regionID, me)
		if len(crash_JSON['market_history'][regionID]) >= my_count:
			thread_print( fmt_name + ' Region Complete!' )
			continue
		i_finished = 0
		i_skipped = 0
		for count,itemID in enumerate(item_list):
			if count % parts <> me: continue
			if thread_exit_flag:
				thread_print( fmt_name + "Received exit signal." )
				return
			last = print_progress_thread()
			#query = 'market/%s/types/%s/history/' % (regionID,itemID)
			query = 'market/{regionID}/history/?{crest_path}inventory/types/{typeID}/'
			query = query.format(
				regionID   = regionID,
				crest_path = crest_path,
				typeID     = itemID
				)
			if str(itemID) in crash_JSON['market_history'][regionID]:
				i_finished = i_finished + 1
				i_skipped = i_skipped + 1
				if i_skipped % 10 == 0:
					thread_print( '{0} skipped {1}'.format(fmt_name, i_skipped) )
				continue #already processed data

			#price_JSON = fetchURL_CREST(query, testserver, debug=False)
			price_JSON = fetchURL_request(query, testserver, debug=False)

			if len(price_JSON['items']) == 0:
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
			i_finished = i_finished + 1

	data_conn.close() # should use a with maybe.

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


def fetchURL_request(query, testserver=False, debug=True):
	fetch_url = ''
	if testserver: fetch_url = '%s%s' % (crest_test_path, query)
	else: fetch_url = '%s%s' % (crest_path, query)
	if debug: thread_print( fetch_url )
	params = {
		'accept-encoding' : 'gzip',
		'user-agent'      : user_agent,
	}
	return_object = {}
	fatal_error = True
	for tries in range (0,retry_limit):
		time.sleep(sleep_timer*tries)
		try:
			request = requests.get( fetch_url,
									params  = params,
									timeout = (default_timeout,default_readtimeout))
			return_object = request.json()
		except requests.exceptions.ConnectionError as e:
			#fatal_error = True
			fatal_error = False
			thread_print('connectionError:%s %s' % (e,fetch_url))
			continue
		except requests.exceptions.ConnectTimeout as e:
			fatal_error = True
			thread_print('connectionTimeout:%s %s' % (e,fetch_url))
			continue
		except requests.exceptions.ReadTimeout as e:
			fatal_error = True
			thread_print('readTimeout:%s %s' % (e,fetch_url))
			continue
		except ValueError:
			fatal_error = True
			thread_print('response not JSON')
			raise

		if request.status_code == requests.codes.ok:
			break
		else:
			fatal_error = False
			if request.status_code == 503:
				threading._sleep(random.random()/8.0)
			else:
				thread_print('HTTPError:%s %s' % (request.status_code,fetch_url))
			continue
	else:
		#thread_print( headers )
		if fatal_error: sys.exit(2)
		return {'items':[]}

	return return_object

def fetchURL_CREST(query, testserver=False, debug=False):
	import random
	#Returns parsed JSON of CREST query
	real_query = ''
	if testserver: real_query = '%s%s' % (crest_test_path, query)
	else: real_query = '%s%s' % (crest_path, query)

	if debug: thread_print( real_query )

	request = urllib2.Request(real_query)
	request.add_header('Accept-Encoding','gzip')
	request.add_header('User-Agent',user_agent)

	headers = {}
	fatal_error = True
	for tries in range (0,retry_limit):
		time.sleep(sleep_timer*tries)
		try:
			opener = urllib2.build_opener()
			raw_response = opener.open(request)
			headers = raw_response.headers
			response = raw_response.read()
		except urllib2.HTTPError as e:
			if e.code == 503:
				threading._sleep(random.random()/8.0)
			else:
				thread_print( 'HTTPError:%s %s' % (e,real_query) )
			fatal_error = False
			continue
		except urllib2.URLError as e:
			thread_print( 'URLError:%s %s' % (e,real_query) )
			fatal_error = True
			continue
		except httplib.HTTPException as e:
			thread_print( 'HTTPException:%s %s' % (e, real_query) )
			fatal_error = True
			continue
		except socket.error as e:
			thread_print( 'Socket Error:%s %s' % (e,real_query) )
			fatal_error = True
			continue

		do_gzip = headers.get('Content-Encoding','') == 'gzip'

		if do_gzip:
			try:
				buf = StringIO.StringIO(response)
				zipper = gzip.GzipFile(fileobj=buf)
				return_result = json.load(zipper)
			except ValueError as e:
				thread_print( "Empty response: Retry %s" % real_query )
				fatal_error = True
				continue
			except IOError as e:
				thread_print( "gzip unreadable: Retry %s" % real_query )
				fatal_error = True
				continue
			else:
				break
		else:
			return_result = json.loads(response)
			break
	else:
		thread_print( headers )
		if fatal_error: sys.exit(2)
		return {'items':[]}

	return return_result

def _date_convert(date_str):
	new_time = datetime.strptime(date_str,'%Y-%m-%dT%H:%M:%S')
	return new_time.strftime('%Y-%m-%d')

def recover_on_restart(region_id, thread_id):
	fmt_name = region_name_format.format( threading.current_thread().name + ":", thread_id )
	crash_filename = "{0}-{1}_{2}".format(region_id, thread_id, crash_filename_base)
	crash_JSON = {}
	try:
		with open(crash_filename,'r') as f:
			crash_JSON = json.load(f)
		if (
				(not 'market_history' in crash_JSON) or
				(not region_id in crash_JSON['market_history'])
			):
			raise Exception("Corrupted recovery file.")
		msg = '%s loaded progress file %s with %s items'
	except Exception as e:
		msg = '%s found no progress file; starting fresh with %s and %s items'
		crash_JSON['market_history'] = {}
		crash_JSON['market_history'][region_id] = {}
	crash_JSON['filename'] = crash_filename
	num_items = len(crash_JSON['market_history'][region_id])
	thread_print( msg % (fmt_name, crash_filename, num_items) )
	return crash_JSON

def write_progress(subtable_name, key1, key2, crash_JSON):
	if subtable_name not in crash_JSON:
		crash_JSON[subtable_name]={}
	if key1 not in crash_JSON[subtable_name]:
		crash_JSON[subtable_name][key1]={}
	crash_JSON[subtable_name][key1][key2]=1

	with open(crash_JSON['filename'],'w') as crash_file:
		json.dump(crash_JSON, crash_file)

def launch_region_threads(regions={}, nthreads=1):
	region_threads = []
	for region_id, region_name in regions.iteritems():
		for n in range(nthreads):
			kwargs = {
				'regions': {region_id: region_name},
				'thread_id': (n, nthreads),
				'debug': False,
				'testserver': False
				}
			new_thread = threading.Thread(
				name="{1}/{0:s}".format(region_name, n),
				kwargs=kwargs,
				target=fetch_markethistory
				)
			new_thread.daemon = False # So they can clean up properly
			region_threads.append(new_thread)
			new_thread.start()
	return region_threads

def wait_region_threads(threads=[]):
	start = datetime.now()
	last = datetime.now()
	while True:
		done = []
		for t in threads:
			if t.is_alive(): t.join(1)
			else: done.append(t.name)
		if done:
			last = print_progress(
				", ".join(done) + " finished. {elapsed:.2f} m total",
				last,
				start,
				len(done),
				len(threads)
				)

		if len(done) == len(threads):
			break

def _optimize_database():
	thread_print( "Optimizing database %s..." % crest_pricehistory )
	data_conn, data_cur = connect_local_databases(db_schema)
	data_cur.execute('''OPTIMIZE TABLE `%s`''' % crest_pricehistory).commit()
	data_conn.close()

def _clean_dir():
	thread_print( "Cleaning up progress/crash files" )
	rm_list = glob.glob('*crest_progress*')
	for file in rm_list:
		os.remove(file)

def main():
	max_threads = thread_count

	threads_per_region = max_threads // len(trunc_region_list)
	_validate_connection()
	region_threads = launch_region_threads(trunc_region_list, threads_per_region)
	wait_region_threads(region_threads)

	if bool_doOptimize:
		_optimize_database()
	_clean_dir()

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise
