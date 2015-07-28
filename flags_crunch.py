#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import requests
import ConfigParser
import pypyodbc
from datetime import datetime

import pandas as pd
from ema_config import *

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
		
def fetch_data(sql_query_fileName, ODBC_connector_name):
	None
	
def split_data(pandas_object, split_column):
	None
	
def getODBC_connection(ODBC_connector_name, 
						host=None, user=None, passwd=None, port=None, db=None):
	bool_easyConnect = False
	if (host	== None and
		user	== None and
		passwd	== None and
		port	== None and
		db		== None):
		bool_easyConnect = True #specific connection info not supplied, use ODBC config
	
def main():
	None
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

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise