#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime
import numpy
import rpy2.robjects as robjects
import rpy2
from rpy2.robjects.packages import importr

import pandas as pd
import pandas.io.sql as psql

from scipy.stats import norm

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