#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
from os import path
import urllib2
import ConfigParser
import pypyodbc
from datetime import datetime
import numpy
import rpy2.robjects as robjects
from rpy2.robjects.packages import importr
#xts = importr("xts", robject_translations = {".subset.xts": "_subset_xts2",  "to.period": "to_period2"})
conf = ConfigParser.ConfigParser()
conf.read(['init.ini','init_local.ini'])

db_host   = conf.get('GLOBALS','db_host')
db_user   = conf.get('GLOBALS','db_user')
db_pw     = conf.get('GLOBALS','db_pw')
db_port   = int(conf.get('GLOBALS','db_port'))
db_schema = conf.get('GLOBALS','db_schema')
db_driver = conf.get('GLOBALS','db_driver')
sde_schema  = conf.get('GLOBALS','sde_schema')

sde_conn  = pypyodbc.connect('DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DATABASE=%s' \
	% (db_driver,db_host,db_port,db_user,db_pw,sde_schema))
sde_cur = sde_conn.cursor()

convert = {}

def main():
	pi = robjects.r['pi']
	print pi
	importr('jsonlite')
	importr('quantmod',robject_translations = {'skeleton.TA':'skeletonTA'})
	importr('data.table')
	robjects.r('''
		query_str <- paste('http://public-crest.eveonline.com/market/10000002/types/',29668,'/history/',sep='')
		print(query_str)
		market.json <- fromJSON(readLines(query_str))
		market.data <- data.table(market.json$items)
	
		market.data <- market.data[,list(Date=as.Date(date),
                                     Volume=volume,
                                     High=highPrice,
                                     Low=lowPrice,
                                     Close=avgPrice[-1], #allow for faked candlesticks
                                     Open=avgPrice)]
		n<-nrow(market.data)
		market.data<-market.data[1:n-1,]
		market.data.ts <- xts(market.data[,-1,with=F],order.by=market.data[,Date],period=7)
		png(paste('plots/',Sys.Date(),'/','PLEX_',Sys.Date(),'.png',sep=''),
			width = 1000,
			height = 600)
		chartSeries(market.data.ts, 
					name=paste(itemid,Sys.Date(),sep='_'),
					TA='addBBands();addVo();addMACD(5,15,5)',
					subset='last 12 weeks')
		
		dev.off()''')
		
if __name__ == "__main__":
	main()