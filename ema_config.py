import ConfigParser
import pypyodbc
import sys
from os import environ, path, getcwd
from datetime import timedelta
import itertools
flatten = itertools.chain.from_iterable

localpath = path.dirname(path.realpath(__file__))
DEV_localpath = path.join(localpath,'init.ini')
ALT_localpath = path.join(localpath,'init_local.ini')

conf = ConfigParser.ConfigParser()
conf.read([DEV_localpath,ALT_localpath])

####GLOBALS####
crest_path = conf.get('CREST','default_path')
crest_test_path = conf.get('CREST','test_path')
user_agent = conf.get('GLOBALS','user_agent')
retry_limit = int(conf.get('GLOBALS','default_retries'))
sleep_timer = int(conf.get('GLOBALS','default_sleep'))
crash_filename_base = conf.get('CREST','progress_file_base')
tick_delay = timedelta(seconds=10)
tick_delay_dbg = timedelta(seconds=5)
default_timeout = int(conf.get('GLOBALS','default_timeout'))
default_readtimeout = int(conf.get('GLOBALS','default_readtimeout'))
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

def sanitize(filename):
	def keep(c):
		return c if (c.isalnum() or c in (' ','.','-')) else ''
	return "".join(keep(c) for c in filename).strip()

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
	'10000032':'Sinq Liaison',
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
	'10000069':'Black Rise',
	'11000031':'Thera'
	}

trunc_region_list = {
	'10000002':'The Forge',
	#'10000043':'Domain',
	#'10000030':'Heimatar',
	#'10000032':'Sinq Laison',
	#'10000042':'Metropolis',
	#'11000031':'Thera'
	}

region_name_maxlen = max( len( r ) for r in region_list.values() ) + 1
region_name_format = "{1}/{0:" + str(region_name_maxlen) + "s}"
