import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
#from os import path, environ, getcwd
import urllib2
import httplib
import requests
import urllib
import MySQLdb 
#ODBC connector not supported on pi/ARM platform
from datetime import datetime, timedelta
import threading
import smtplib	#for emailing logs 

from ema_config import *
thread_exit_flag = False

db_con = None
db_cur = None

def main():
	None

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise