#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import requests
import ConfigParser
import pypyodbc
from datetime import datetime

import pandas as pd
from ema_config import *

from flags_crunch import Flag

def main():
	None

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True
		raise