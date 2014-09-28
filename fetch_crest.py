#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime

conf = ConfigParser.ConfigParser()
conf.read(['init.ini','init_local.ini'])
####GLOBALS####
crest_path = conf.get('CREST','default_path')
crest_test_path = conf.get('CREST','test_path')
user_agent = conf.get('GLOBALS','user_agent')
retry_limit = int(conf.get('GLOBALS','default_retries'))
sleep_timer = int(conf.get('GLOBALS','default_sleep'))
crash_filename = conf.get('CREST','progress_file')
crash_JSON = None

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
	
def fetch_markethistory(trunc_regions=False, debug=False, testserver=False):
	sde_cur.execute('''SELECT typeid
					FROM invtypes conv
					JOIN invgroups grp ON (conv.groupID = grp.groupID)
					WHERE marketgroupid IS NOT NULL
					AND conv.published = 1
					AND grp.categoryid NOT IN (9,16,350001,2)
					AND grp.groupid NOT IN (30,659,485,485,873,883)''') #remove typeid NOT IN eventualy
	item_list_tmp = sde_cur.fetchall()
	item_list = []
	for row in item_list_tmp:
		item_list.append(row[0])
	#if debug: print item_list
	
	price_history_headers = []
	data_cur.execute('''SHOW COLUMNS FROM `%s`''' % crest_pricehistory)
	table_info = data_cur.fetchall()
	#TODO: If len(table_info) == 0 exception
	for column in table_info:
		price_history_headers.append(column[0])
	
	todo_region_list = {}
	if trunc_regions:	todo_region_list = trunc_region_list
	else: 				todo_region_list = region_list
	
	for regionID,regionName in todo_region_list.iteritems():
		print regionName
		
		try:
			if len(crash_JSON['market_history'][str(regionID)]) >= len(item_list):
				print '\tRegion Complete'
		except KeyError as e:
			None
		
		for itemID in item_list:
			query = 'market/%s/types/%s/history/' % (regionID,itemID)
			try:
				if str(itemID) in crash_JSON['market_history'][str(regionID)]:
					if debug: print '%s:\tskip' % query
					continue #already processed data
			except KeyError as e:
				None				
			
			
			price_JSON = fetchURL_CREST(query, testserver)
			
			#TODO: 0-fill missing dates
			if len(price_JSON['items']) == 0: 
				write_progress('market_history',regionID,itemID)
				if debug: print '%s:\tEMPTY' % query
				continue
			if debug: print query
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
			write_progress('market_history',regionID,itemID)

def writeSQL(db_cur, table, headers_list, data_list, hard_overwrite=True, debug=False):
	insert_statement = '''INSERT INTO %s (%s) VALUES''' % (table, ','.join(headers_list))
	if debug:
		print insert_statement
	for entry in data_list:
		value_string = ''
		for value in entry:
			if isinstance(value, (int,long,float)): #if number, add value
				value_string = '%s,%s' % ( value_string, value)
			else:		#if string value: add 'value'
				if value == None:
					value_string = '%s,NULL' % ( value_string)
				else:
					value = value.replace('\'', '\\\'') #sanitize apostrophies
					value_string = '%s,\'%s\'' % ( value_string, value)
		value_string = value_string[1:]
		if debug:
			print value_string
		insert_statement = '%s (%s),' % (insert_statement, value_string)
	
	
	insert_statement = insert_statement[:-1]	#pop off trailing ','
	if hard_overwrite:
		duplicate_str = '''ON DUPLICATE KEY UPDATE '''
		for header in headers_list:
			duplicate_str = "%s %s=%s," % (duplicate_str, header, header)
		
		insert_statement = "%s %s" % (insert_statement, duplicate_str)
		insert_statement = insert_statement[:-1]	#pop off trailing ','
	if debug:
		print insert_statement
	db_cur.execute(insert_statement)
	db_cur.commit()
	
def fetchURL_CREST(query, testserver=False, debug=False):
	#Returns parsed JSON of CREST query
	real_query = ''
	if testserver:	real_query = '%s%s' % (crest_test_path, query)
	else: 			real_query = '%s%s' % (crest_path, query)
	
	if debug: print real_query
	
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
			print 'HTTPError:%s %s' % (e,real_query)
			continue
		except urllib2.URLError as e:
			print 'URLError:%s %s' % (e,real_query)
			continue
		except socket.error as e:
			print 'Socket Error:%s %s' % (e,real_query)
			continue
		
		do_gzip = False
		try:
			if headers['Content-Encoding'] == 'gzip':
				do_gzip = True
		except KeyError as e:
			None
				
		if do_gzip:
			try:
				buf = StringIO.StringIO(response)
				zipper = gzip.GzipFile(fileobj=buf)
				return_result = json.load(zipper)
			except ValueError as e:
				print "Empty response: retry %s" % real_query
				continue
			except IOError as e:
				print "gzip unreadable: Retry %s" %real_query
				continue
			else:
				break
		else:
			return_result = json.loads(response)
			break
	else:
		print headers
		sys.exit(2)
	
	return return_result
	
def _date_convert(date_str):
	new_time = datetime.strptime(date_str,'%Y-%m-%dT%H:%M:%S')
	return new_time.strftime('%Y-%m-%d')

def recover_on_restart():
	global crash_JSON
	try:
		tmp_filehandle = open(crash_filename,'r')
		crash_JSON = json.load(tmp_filehandle)
	except Exception as e:
		crash_JSON = {}
		print 'No recovery file found, starting fresh'
		try:
			tmp_filehandle.close()
		except Exception as e:
			None
		return
	
	print 'Loading progress file %s' % crash_filename
	tmp_filehandle.close()
	return
	
def write_progress(subtable_name, key1, key2):
	global crash_JSON
	if subtable_name not in crash_JSON:
		crash_JSON[subtable_name]={}
		crash_JSON[subtable_name][key1]={}
		crash_JSON[subtable_name][key1][key2]=0
	if key1 not in crash_JSON[subtable_name]:
		crash_JSON[subtable_name][key1]={}
	crash_JSON[subtable_name][key1][key2]=1
	
	with open(crash_filename,'w') as crash_file:
		json.dump(crash_JSON,crash_file)
	
def main():
	_validate_connection()
	
	recover_on_restart()
	###FETCH PRICEHISTORY###
	print "FETCHING CREST/MARKET_HISTORY"
	fetch_markethistory(True,True)
	
if __name__ == "__main__":
	main()