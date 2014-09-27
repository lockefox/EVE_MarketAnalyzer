#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime

conf = ConfigParser.ConfigParser()
conf.read(['init.ini','init_local.ini'])

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
		
def main():
	_validate_connection()
	
	###FETCH PRICEHISTORY###
	
if __name__ == "__main__":
	main()