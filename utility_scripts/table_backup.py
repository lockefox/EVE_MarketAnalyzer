import sys, gzip, StringIO, sys, math, getopt, time, json, socket
#from os import path, environ, getcwd
import pypyodbc
import ConfigParser
from datetime import datetime, timedelta
from os import environ, path, getcwd, chdir
thread_exit_flag = False

### PATH STUFF ###
local_path = path.dirname( path.realpath(__file__) )
archive_config_path = path.join( local_path, "archive_config.json" )
config_info = json.load( open(archive_config_path) )

base_path = path.split( path.dirname( path.realpath(__file__) ) )[0]

sql_file_path = path.join( base_path,'SQL' )
DEV_localpath = path.join( base_path,'init.ini' )
ALT_localpath = path.join( base_path,'init_local.ini' )
conf = ConfigParser.ConfigParser()
conf.read( [DEV_localpath, ALT_localpath] )

#### PROGRAM GLOBALS ####
run_arg = conf.get( 'ARCHIVE', 'default_run_arg' )
gDebug = False
nowTime = datetime.now()

def test_connection ( odbc_dsn, table_name, table_create_file, debug=gDebug ):
	try:
		db_con = pypyodbc.connect( 'DSN=%s' % odbc_dsn )
		db_cur = db_con.cursor()
	except Exception as e:
		print "***ERROR: unable to connect to DSN=%s: %s" % ( odbc_dsn, e )
		sys.exit(2)
	
	test_query = '''SELECT * FROM {table_name} LIMIT 1'''
	test_query = test_query.format( table_name = table_name )	
	if debug: print "\t%s" % test_query
	try:
		db_cur.execute( test_query )
	except Exception as e:
		print "***ERROR: test_query failed"
		if table_create_file:
			try:
				print "***Trying to CREATE TABLE %s.%s" % ( odbc_dsn, table_name )
				create_table( db_con, db_cur, table_create_file, debug )
				db_cur.execute( test_query )	#retry test just in case
			except Exception as err:
				print "***ERROR: unable to create table %s.%s %s" % ( odbc_dsn, table_name, err)
				sys.exit(2)
		else:
			print "\tNo table_create_file given, exiting"
			sys.exit(2)
	db_con.close()
	print "\t%s.%s READY" % ( odbc_dsn, table_name )

def create_table ( db_con, db_cur, table_create_file, debug=gDebug ):
	global sql_file_path
	table_filepath = path.join( sql_file_path, table_create_file ) 
	if debug: print table_filepath 
	table_create_query = open( table_filepath ).read()
	table_create_commands = table_create_query.split(';')
	try:
		for command in table_create_commands:
			if debug: print "\t%s" % command
			db_cur.execute( command )
			db_con.commit()
	except Exception as e:
		if e[0] == "HY090":
			print "***Suppressed HY090 ERR: Invalid string or buffer length"
			### environment issue.  creates table, but SQLite ODBC connector has a driver issue ###
			### If table is not created, test read will fail ###
		else:
			raise

def check_table_contents ( odbc_dsn, table_name, date_range, debug ):
	maxDate = nowTime - timedelta ( days=date_range )
	max_date_str = maxDate.strftime( "%Y-%m-%d" )
	date_str = max_date_str	#default to max range
	
	db_con = pypyodbc.connect( 'DSN=%s' % odbc_dsn )
	db_cur = db_con.cursor()
	
	query_str = ''' SELECT max(price_date) FROM {table_name}'''#price_date needs to remain standardized!
	query_str = query_str.format( table_name = table_name )
	if debug: print "\t%s" % query_str
	dbValue = db_cur.execute(query_str).fetchall()
	
	if dbValue[0][0]:	#if any value returned from query
		if debug: print "\t%s" % dbValue
		date_str = dbValue[0][0]
	else:
		date_str = max_date_str #explicitly pull maximum data
	
	db_con.close()
	return date_str

def validate_query ( query, date_str, table_name ):
	return_str = ""
	
	if query.upper() == "ALL":
		return_str = '''SELECT * FROM %s''' % ( table_name )
	if "WHERE" in query.upper():
		return_str = '''SELECT * FROM %s %s AND price_date>\'%s\'''' % ( table_name, query, date_str )
	else:
		return_str = '''SELECT * FROM %s WHERE %s AND price_date>\'%s\'''' % ( table_name, query, date_str )
		
	return return_str

def pull_data ( odbc_dsn, table_name, query, date_str, debug ):
	db_con = pypyodbc.connect( 'DSN=%s' % odbc_dsn )
	db_cur = db_con.cursor()
	
	query_str = validate_query( query, date_str, table_name )
	if debug: print "\t%s" % query_str
	db_cur.execute( query_str )
	dataObj = db_cur.fetchall()
	
	db_con.close()
	return dataObj

def write_data ( odbc_dsn, table_name, dataObj, debug ):
	table_headers = config_info['tables'][table_name]['cols']
	column_types  = config_info['tables'][table_name]['types']	#TODO: this is probably bad
	
	write_str = '''INSERT INTO {table_name} ({table_headers}) VALUES'''
	write_str = write_str.format(
					table_name    = table_name,
					table_headers = ','.join(table_headers)
					)
	
	if debug: print write_str 
	value_str = ""
	print_single = 0
	for row in dataObj:
		col_index = 0	#for tracking headers
		data_values = []
		for col in row:
			header   = table_headers[col_index]
			dataType = column_types[header].lower()
			data_str = ''
			
			if dataType == 'number':
				data_str = str(col)
			elif dataType == 'string':
				data_str = "'%s'" % col
			else:
				print "***unsupported dataType: header=%s dataType=%s" % (header, dataType)
			
			data_values.append(data_str)
			col_index += 1
		tmp_value_str = ','.join(data_values)
		tmp_value_str = tmp_value_str.rstrip(',')
		#if debug: print tmp_value_str
		value_str = "%s (%s)," % (value_str, tmp_value_str)
		
		if print_single < 1 and debug:
			print value_str
			print_single += 1
	value_str = value_str.rstrip(',')
	
	duplicate_str = ""
	#duplicate_str = '''ON DUPLICATE KEY UPDATE '''
	#for header in table_headers:
	#	duplicate_str = "%s %s=%s," % (duplicate_str, header, header)
	#	
	#duplicate_str = duplicate_str.rstrip(',')
	#if debug: print duplicate_str
	commit_str = '''{write_str} {value_str} {duplicate_str}'''
	commit_str = commit_str.format(
					write_str     = write_str,
					value_str     = value_str, 
					duplicate_str = duplicate_str
					)
	writeSQL ( odbc_dsn, commit_str, debug )
	
def writeSQL ( odbc_dsn, commit_str, debug ):
	db_con = pypyodbc.connect( 'DSN=%s' % odbc_dsn )
	db_cur = db_con.cursor()
	
	db_cur.execute(commit_str).commit()
	
	db_con.close()
	
def main():
	global run_arg
	global gDebug
	### Load MAIN args ###
	try:
		opts, args = getopt.getopt( sys.argv[1:], 'h:l', ['config=', 'debug'] )
	except getopt.GetoptError as e:
		print str(e)
		print 'unsupported argument'
		sys.exit()
	for opt, arg in opts:
		if opt == '--config':
			run_arg = arg
		elif opt == '--debug':
			gDebug=True
		else:
			assert False

	debug = gDebug
	
	try:
		config_info['args'][run_arg]
	except KeyError as e:
		print "Invalid config '%s'" % run_arg
		sys.exit(2)
	print "running profile: %s" % run_arg	
	if debug: print "archive_config_path = %s" % archive_config_path
	if debug: print "sql_file_path = %s" % sql_file_path	
	if debug: print config_info['args'][run_arg]
	
	### Config information ###
	export_import          =      config_info['args'][run_arg]['export_import'].lower()
	destination_DSN        =      config_info['args'][run_arg]['destination_DSN']
	destructive_write      = int( config_info['args'][run_arg]['destructive_write'] )
	clean_up_archived_data = int( config_info['args'][run_arg]['clean_up_archived_data'] )
	
	### Run through archive operations ###
	for table_name,info_dict in config_info['args'][run_arg]['tables_to_run'].iteritems():
		print "--Preparing to back up: %s" % table_name
		ODBC_DSN   = info_dict['ODBC_DSN']
		create     = info_dict['create']
		query      = info_dict['query']
		date_range = info_dict['date_range']
		
		print "--Testing Connections"
		read_DSN  = ""
		write_DSN = ""
		if export_import == 'export':
			read_DSN  = ODBC_DSN
			write_DSN = destination_DSN
		elif export_import == 'import':
			read_DSN  = destination_DSN
			write_DSN = ODBC_DSN
		else:
			print "***Unsupported export_import value: %s" % export_import
			sys.exit(2)
		
		print "\tREAD = %s.%s" % ( read_DSN, table_name )
		test_connection( read_DSN, table_name,      "", debug )
		print "\tWRITE = %s.%s" % ( write_DSN, table_name )
		test_connection( write_DSN, table_name, create, debug )
		
		print "--Testing %s.%s for existing data" % ( write_DSN, table_name )
		date_str = check_table_contents ( write_DSN, table_name, date_range, debug )
		
		print "--Fetching data after %s on %s.%s" % ( date_str, read_DSN, table_name )
		dataObj = pull_data( read_DSN, table_name, query, date_str, debug )
		if debug: print  dataObj[0]
	
		print "--Writing data out to archive %s.%s" % ( write_DSN, table_name )

		write_data ( write_DSN, table_name, dataObj, debug )
		
		
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise