#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import requests
import ConfigParser
import pypyodbc
from datetime import datetime

import pandas as pd
import pandas.io.sql as psql

from ema_config import *

### MODELS ###
import models_crest

class Flag(object):
	def __init__ (self, typeID, typeName):
		self.typeID		= -1
		self.typeName	= ''
		self.flags 		= {} 	#modelName : flagMagnitude
		self.date		= ''	#YYYY-MM-DD
		self.time		= None	#TODO: expand for intra-day tracking
	def __str__ (self):	#debug printer
		return_str = '%s:%s\n' % (self.typeID, self.typeName)
		for model, value in self.flags.iteritems():
			return_str = '%s\n\t%s:%s\n' % (return_str, model, value)	#Make easier to read?
		return return_str
####	typeID:typeName
####		modelName1:magnitude1
####		modelName2:magntitude2
		
	def addFlag (self, modelName, flagMagnitude):
		self.flags[modelName] = flagMagnitude

	def writeToSQL(self, odbc_connection, odbc_cursor):
		None
		#TODO: write ODBC writter.  
		
def fetch_data(sql_query_fileName, ODBC_connector_name, date_key="", debug=True):
	local_con = None
	local_cur = None
	query_filePath = '%s/SQL/%s' % (localpath, sql_query_fileName)	

	if debug: print "\tConnecting DB: %s" % ODBC_connector_name
	local_con = pypyodbc.connect('DSN=%s' % ODBC_connector_name)
	local_cur = local_con.cursor()
	
	if debug: print "\tReading query: %s" % sql_query_fileName
	query = open(query_filePath).read()

	if debug: print "\t--Fetching data--"
	return_df = psql.read_sql(query, local_con)
		
	#Clean up after yourself#
	local_con.close()
	local_cur.close()
	return return_df
	
	
def main():
############
#	#pseudocode framework
#	pd_CREST_data = fetch_data("STAT_CRESTdata", "crest_ODBC") #returns pandas dataframe loaded from SQL
#	crest_data_todo = [] #array of dataframes for threading
#	crest_data_todo = split_data(pd_CREST_data, "typeid")
#
#	for _THREAD_ in _THREADPOOL_:
#		##fetch crest_data_todo[progress % threadID] #thread fetches sublist of items to crunch
#		Flag_obj = Flag(typeID, typeName)
#		pd_singleType_data 
#		
#		Flag_obj.addFlag(CRESTmodel.price_smm(pd_singleType_data))
#		Flag_obj.addFlag(CRESTmodel.price_sma(pd_singleType_data))
#		Flag_obj.addFlag(CRESTmodel.price_vol(pd_singleType_data))
#		Flag_obj.writeToSQL(_THREAD_odbc_connection, _THREAD_odbc_cursor)

	crest_data = fetch_data("query_CRESTstats.mysql", conf.get('NEWSTATS','CREST_ODBC_DSN'))
	print crest_data
	
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise