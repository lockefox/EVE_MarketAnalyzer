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
	global convert
	#pi = robjects.r['pi']
	#print pi
	
	###R Modules###
	#Must be installed in R first. 
	importr('jsonlite')
	importr('quantmod',robject_translations = {'skeleton.TA':'skeletonTA'})
	importr('data.table')
	
	R_config_file = open(conf.get('STATS','R_config_file'),'r')
	R_todo = json.load(R_config_file)
	
	#R_date = robjects.r('Sys.Date()')
	#print str(R_date)
	today = datetime.now().strftime('%Y-%m-%d')
	if not os.path.exists('plots/%s' % today):
		os.makedirs('plots/%s' % today)
		
	sde_cur.execute('''SELECT typeid,typename
						FROM invtypes conv
						JOIN invgroups grp ON (conv.groupID = grp.groupID)
						WHERE marketgroupid IS NOT NULL
						AND conv.published = 1''')
	tmp_convlist = sde_cur.fetchall()
	for row in tmp_convlist:
		convert[row[0]]=row[1]
	today = datetime.now().strftime('%Y-%m-%d')
	print today	
	sys.exit()
	for group,item_list in R_todo['forced_plots'].iteritems():
		dump_path = 'plots\\%s\\%s' % (R_date,group)
		if not os.path.exists(dump_path):
			os.makedirs(dump_path)
		sys.exit()
	robjects.r('''
		query_str <- paste('http://public-crest.eveonline.com/market/10000002/types/',29668,'/history/',sep='')
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