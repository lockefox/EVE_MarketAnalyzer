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

def _validate_connection(
		tables=[ema_reprocess,ema_build],
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


def _reprocess():
        print "Calculating reprocessed value for items....."

        refinables_query = '''insert into ema_reprocess(select price_date,t.typeID,p.regionid,IF(categoryid=25,0.7236,0.55)*SUM(quantity*avgPrice/portionsize) from
        %.invTypes t,%.invGroups g,%.invTypeMaterials m,crest_markethistory as p
        where t.groupid=g.groupid and t.typeid=m.typeid and m.materialtypeid=p.itemid 
        and price_date>(select  CASE WHEN max(value_date) IS NULL THEN 0 ELSE max(value_date) END from ema_reprocess) 
        group by price_date,t.typeid,p.regionid 
        having count(*)=(select count(*) from %.invTypeMaterials where typeid=t.typeid))'''

	
        refinables_query = refinables_query.replace('%', sde_schema)
	
       
        data_conn, data_cur = connect_local_databases(db_schema)
	data_cur.execute(refinables_query).commit()
        data_conn.close()


def _build():
        print "Calculating material cost of items (ME10)....."
        
        material_cost_query = '''insert into ema_build(select price_date,t.typeid,p.regionid,SUM(CEILING(ROUND(0.9*m.quantity,2))*avgprice/pr.quantity)
        from %.invTypes t, %.industryActivityProducts pr, %.industryActivityMaterials m, crest_markethistory p
        where t.typeid=pr.producttypeid and pr.typeid=m.typeid and m.materialtypeid=p.itemid and pr.activityid=1 and m.activityid=1
        and price_date>(select CASE WHEN max(value_date) IS NULL THEN 0 ELSE max(value_date) END from ema_build)
        group by price_date,t.typeid,p.regionid 
        having count(*)=(select count(*) from %.industryActivityProducts pra, %.industryActivityMaterials ma
        where ma.typeid=pra.typeid and pra.producttypeid=t.typeid and pra.activityid=1 and ma.activityid=1))'''
        
	
        material_cost_query = material_cost_query.replace('%', sde_schema)
	
       
        data_conn, data_cur = connect_local_databases(db_schema)
	data_cur.execute(material_cost_query).commit()
        data_conn.close()

def main():
	#max_threads = thread_count
	#threads_per_region = max_threads // len(trunc_region_list)
	_validate_connection()
	#region_threads = launch_region_threads(trunc_region_list, threads_per_region)
	#wait_region_threads(region_threads)
	_reprocess()
        _build()
        
        
	#_clean_dir()

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		
		raise
