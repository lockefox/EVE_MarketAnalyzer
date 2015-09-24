import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
#from os import path, environ, getcwd
import urllib2
import httplib
import requests
import urllib
from datetime import datetime, timedelta

from ema_config import *
thread_exit_flag = False

def main():
	None

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise