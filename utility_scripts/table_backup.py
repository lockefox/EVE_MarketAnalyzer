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
run_arg     =      conf.get( 'ARCHIVE', 'default_run_arg' )
erase_delay = int( conf.get( 'ARCHIVE', 'erase_delay' ) )
write_mod   = int( conf.get( 'ARCHIVE', 'write_mod' ) )
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
		return_str = '''SELECT * FROM %s WHERE %s''' % ( table_name, date_str )
	elif date_str:
		if "WHERE" in query.upper():
			return_str = '''SELECT * FROM %s %s AND %s''' % ( table_name, query, date_str )
		else:
			return_str = '''SELECT * FROM %s WHERE %s AND %s''' % ( table_name, query, date_str )
	else: #in case no date_str is given (or otherwise blank/None
		if "WHERE" in query.upper():
			return_str = '''SELECT * FROM %s %s''' % ( table_name, query )
		else:
			return_str = '''SELECT * FROM %s WHERE %s''' % ( table_name, query )
	
	#if query.upper() == "ALL":
	#	return_str = '''SELECT * FROM %s''' % ( table_name )
	#elif date_str:
	#	if "WHERE" in query.upper():
	#		return_str = '''SELECT * FROM %s %s AND price_date>\'%s\'''' % ( table_name, query, date_str )
	#	else:
	#		return_str = '''SELECT * FROM %s WHERE %s AND price_date>\'%s\'''' % ( table_name, query, date_str )
	#else: #in case no date_str is given (or otherwise blank/None
	#	if "WHERE" in query.upper():
	#		return_str = '''SELECT * FROM %s %s''' % ( table_name, query )
	#	else:
	#		return_str = '''SELECT * FROM %s WHERE %s''' % ( table_name, query )
	

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
	
	data_to_process = len( dataObj )
	local_write_mod = write_mod
	if write_mod == -1:
		local_write_mod = data_to_process+10
		
	write_str = '''INSERT INTO {table_name} ({table_headers}) VALUES'''
	write_str = write_str.format(
					table_name    = table_name,
					table_headers = ','.join(table_headers)
					)
	
	if debug: print write_str 
	value_str = ""
	print_single = 0
	row_count = 0
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
			
		if row_count % local_write_mod == 0:
			if debug: print "Processed %s of %s\twriting to %s.%s" % ( row_count, data_to_process, odbc_dsn, table_name )
			commit_str = '''{write_str} {value_str}'''
			commit_str = commit_str.format(
							write_str = write_str,
							value_str = value_str.rstrip(',')
							)
			writeSQL ( odbc_dsn, commit_str, debug )
			value_str = ""
		row_count += 1
	
	if debug: print "Processed %s of %s\twriting to %s.%s" % ( row_count, data_to_process, odbc_dsn, table_name )
	commit_str = '''{write_str} {value_str}'''
	commit_str = commit_str.format(
					write_str = write_str,
					value_str = value_str.rstrip(',')
					)
	writeSQL ( odbc_dsn, commit_str, debug )
	
	#duplicate_str = ""
	#duplicate_str = '''ON DUPLICATE KEY UPDATE '''
	#for header in table_headers:
	#	duplicate_str = "%s %s=%s," % (duplicate_str, header, header)
	#	
	#duplicate_str = duplicate_str.rstrip(',')
	#if debug: print duplicate_str
	#commit_str = '''{write_str} {value_str} {duplicate_str}'''
	#commit_str = commit_str.format(
	#				write_str     = write_str,
	#				value_str     = value_str, 
	#				duplicate_str = duplicate_str
	#				)
	#if debug: print "\twriting data to %s.%s" % ( odbc_dsn, table_name )
	#writeSQL ( odbc_dsn, commit_str, debug )
	
def writeSQL ( odbc_dsn, commit_str, debug ):
	db_con = pypyodbc.connect( 'DSN=%s' % odbc_dsn )
	db_cur = db_con.cursor()
	
	db_cur.execute(commit_str).commit()
	
	db_con.close()
	
def delete_data ( odbc_dsn, table_name, before_or_after, query, date_str, debug ):
	print "***Preparing to delete data %s %s in %s.%s***" % ( before_or_after, date_str, odbc_dsn, table_name )
	#time.sleep(erase_delay) #Allow user a moment to escape erase
	
	date_test = ""
	if   before_or_after.lower() == "before":
		date_test = "price_date<"
	elif before_or_after.lower() == "after":
		date_test = "price_date>"
	else:
		print "***ERROR: Unsupported before_or_after value: %s" % ( before_or_after )
		sys.exit(2)
		
	query_str = ""
	if query:
		query_str = '''DELETE FROM {table_name} WHERE {query} AND {date_test}{date_str}'''
		query_str = query_str.format(
						table_name = table_name,
						query      = query, 
						date_test  = date_test,
						date_str   = date_str
						)
	else:
		query_str = '''DELETE FROM {table_name} WHERE {date_test}{date_str}'''
		query_str = query_str.format(
						table_name = table_name,
						date_test  = date_test,
						date_str   = date_str
						)						
	if debug: print "\t%s" % query_str
	
	writeSQL( odbc_dsn, query_str, debug ) 

def get_min_date ( odbc_dsn, table_name, query, debug ):
	query_str = '''SELECT MIN(price_date) FROM {table_name}'''
	query_str = query_str.format(
					table_name = table_name 
				)
				
	if query:
		query_str = '''%s WHERE %s''' % ( query_str, query )
	
	if debug: print query_str
	db_con = pypyodbc.connect( 'DSN=%s' % odbc_dsn )
	db_cur = db_con.cursor()
	
	db_cur.execute(query_str)
	
	table_data = db_cur.fetchall()
	
	db_con.close()
	return table_data[0][0]
	
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
	export_import     =      config_info['args'][run_arg]['export_import'].lower()
	destination_DSN   =      config_info['args'][run_arg]['destination_DSN']
	try:	#if not defined, assume no erasing!
		clean_up_READ     = int( config_info['args'][run_arg]['clean_up_READ'] )
	except KeyError as e:
		clean_up_READ = 0
	try: #if not defined, assume no erasing!
		clean_up_WRITE    = int( config_info['args'][run_arg]['clean_up_WRITE'] )
	except KeyError as e:
		clean_up_WRITE = 0
	
	warning_override = False
	### Run through archive operations ###
	for table_name,info_dict in config_info['args'][run_arg]['tables_to_run'].iteritems():
		print "--Preparing to back up: %s" % table_name
		ODBC_DSN   = info_dict['ODBC_DSN']
		create     = info_dict['create']
		query      = info_dict['query']
		date_range = info_dict['date_range']
		try:
			sub_date_range = int( info_dict['sub_date_range'] )
		except KeyError as e:
			sub_date_range = date_range
			
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
		
		if debug: 
			user_ack = raw_input( "--CONFIG CORRECT? (y/n)"  )
			user_ack = user_ack.rstrip('\n')
			if user_ack.lower() != "y" :	#or warning_override
				sys.exit(0)
				
		print "--Testing %s.%s for existing data" % ( write_DSN, table_name )
		date_str = check_table_contents ( write_DSN, table_name, date_range, debug )
		if query.upper() == "ALL":
			prev_date_str = date_str
			maxDate = nowTime - timedelta ( days=date_range )
			date_str = maxDate.strftime( "%Y-%m-%d" )
			print "***query=ALL, overriding date: from=%s to=%s" % ( prev_date_str, date_str )
		
		date_str_dateTime = datetime.strptime( date_str, "%Y-%m-%d" )
		total_range = nowTime - date_str_dateTime

		date_query = ""
		max_dateTime = date_str_dateTime
		min_dateTime = date_str_dateTime
		while max_dateTime < nowTime:
			max_dateTime = max_dateTime + timedelta( days=sub_date_range ) #increment max_date
			max_date_str = max_dateTime.strftime( "%Y-%m-%d" )
			min_date_str = min_dateTime.strftime( "%Y-%m-%d" )
			
			print "--Fetching data between %s and %s on %s.%s" % ( min_date_str, max_date_str, read_DSN, table_name )
			date_query = '''(price_date > \'%s\' AND price_date <= \'%s\')''' % ( min_date_str, max_date_str )
			if debug: print "\t%s" % date_query
			dataObj = pull_data( read_DSN, table_name, query, date_query, debug )
			
			if dataObj:
				print "--Writing data out to archive %s.%s" % ( write_DSN, table_name )
				if debug: print  dataObj[0]
				write_data ( write_DSN, table_name, dataObj, debug )
			else:
				print "***READ and WRITE tables already synchronized***"
		
			min_dateTime = min_dateTime + timedelta( days=sub_date_range ) #increment min_date
			
		maxDate = nowTime - timedelta ( days=date_range )
		cleanup_date_str = maxDate.strftime( "%Y-%m-%d" )
		if clean_up_READ == 1:	#Cleans table archive was written FROM
			print "***WARNING***Staged to delete archive data!!!"
			before_or_after = "after"
			user_ack = "" 
			#TODO: ADD OVERRIDE FOR AUTOMATED ARCHIVE MANAGEMENT
			user_ack = raw_input( "--SYSTEM WILL DELETE ARCHIVED DATA: AFTER %s IN %s.%s (y/n)" % ( cleanup_date_str, read_DSN, table_name ) )
			user_ack = user_ack.rstrip('\n')
			if user_ack.lower() == "y" :	#or warning_override
				if debug: print date_str
				date_str_dateTime = datetime.strptime( cleanup_date_str, "%Y-%m-%d" )
				total_range = nowTime - date_str_dateTime
		
				date_query = ""
				max_dateTime = nowTime
				step_dateTime = date_str_dateTime
				min_dateTime = date_str_dateTime
				while step_dateTime < max_dateTime:	#run forward
					step_dateTime = step_dateTime + timedelta( days=sub_date_range ) #increment max_date
					max_date_str  = step_dateTime.strftime( "%Y-%m-%d" )
					min_date_str  = min_dateTime.strftime( "%Y-%m-%d" )
					query_date_str = '''(price_date > \'%s\' AND price_date <= \'%s\')''' % ( min_date_str, max_date_str )
					if debug: print query_date_str
					delete_data ( read_DSN, table_name, before_or_after, query, query_date_str, debug )
					min_dateTime = min_dateTime + timedelta( days=sub_date_range ) #increment min_date
			else:
				print "----user aborted delete operation.  No data affected in %s.%s" % ( read_DSN, table_name )
		
		if clean_up_WRITE == 1:	#Cleans table archive was written TO
			print "***WARNING***Staged to delete archive data!!!"
			before_or_after = "before"
			#TODO: ADD OVERRIDE FOR AUTOMATED ARCHIVE MANAGEMENT
			user_ack = raw_input( "--SYSTEM WILL DELETE ARCHIVED DATA: BEFORE %s IN %s.%s (y/n)" % ( cleanup_date_str, write_DSN, table_name ) )
			user_ack = user_ack.rstrip('\n')
			if user_ack.lower() == "y" :	#or warning_override
				min_date_str = get_min_date ( write_DSN, table_name, query, debug )
				
				min_dateTime = datetime.strptime( min_date_str, "%Y-%m-%d" )
				max_dateTime = datetime.strptime( cleanup_date_str, "%Y-%m-%d" )
				step_dateTime = max_dateTime
				while step_dateTime > min_dateTime:		#run backward
					step_dateTime = step_dateTime - timedelta( days=sub_date_range ) #increment max_date
					max_date_str = max_dateTime.strftime( "%Y-%m-%d" )
					min_date_str = step_dateTime.strftime( "%Y-%m-%d" )
					query_date_str = '''(price_date >= \'%s\' AND price_date < \'%s\')''' % ( min_date_str, max_date_str )
					if debug: print query_date_str
					delete_data ( write_DSN, table_name, before_or_after, query, query_date_str, debug )
					max_dateTime = max_dateTime - timedelta( days=sub_date_range ) #increment min_date
			else:
				print "----user aborted delete operation.  No data affected in %s.%s" % ( write_DSN, table_name )
		
		
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise