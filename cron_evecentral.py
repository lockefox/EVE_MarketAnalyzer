import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path, environ
import urllib2
import pypyodbc
from datetime import datetime, timedelta
import threading

from ema_config import *
thread_exit_flag = False

def thread_print(msg):
	sys.stdout.write("%s\n" % msg)
	sys.stdout.flush()
	
def query_locationType(locationID):
	#Returns supported query modifier.  Else blank to avoid bad calls
	digit = str(locationID)[-1]
	int_digit = int(digit)
	
	if int_digit == 1:
		return 'regionlimit=%s' % locationID
	#elif  int_digit == 2:
	#	None #constellation not supported
	elif int_digit == 3:
		return 'usesystem=%s' % locationID
	elif int_digit == 6:
		return 'usestation=%s' % locationID
	else:
		return ''
def fetch_typeIDs():
	None
	
def _initSQL(table_name):
	None
	
def main:
	None
if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise