import sys, gzip, StringIO, sys, math, getopt, time, json, socket
#from os import path, environ, getcwd
import pypyodbc
import ConfigParser
from datetime import datetime, timedelta
from os import environ, path, getcwd, chdir
import smtplib	#for emailing logs 
thread_exit_flag = False

def main():
	None

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		thread_exit_flag = True