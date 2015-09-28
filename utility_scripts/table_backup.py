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
		None

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise