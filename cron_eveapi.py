import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket, platform
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
from eveapi import eveapi

from ema_config import *
thread_exit_flag = False
