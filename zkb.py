#!/Python27/python.exe

import sys, gzip, StringIO, sys, math, os, getopt, time, json, socket
import urllib2
import MySQLdb
import ConfigParser
from datetime import datetime

try:
	import stomp	#for live connections later
	CANSTOMP = True
except ImportError:
	CANSTOMP = False

conf = ConfigParser.ConfigParser()
conf.read(["init.ini", "init_local.ini"])

base_query = conf.get("ZKB","base_query")
query_limit = int(conf.get("ZKB","query_limit"))
subquery_limit = int(conf.get("ZKB","subquery_limit"))
retry_limit = int(conf.get("ZKB","retry_limit"))
default_sleep = int(conf.get("ZKB","default_sleep"))
User_Agent = conf.get("GLOBALS","user_agent")
logfile = conf.get("ZKB","logfile")
result_dumpfile = conf.get("ZKB","result_dumpfile")

sleepTime = query_limit/(24*60*60)

log = open (logfile, 'a+')

valid_modifiers = (
	"kills",
	"losses",
	"w-space",
	"solo",
	"no-items",
	"no-attackers",
	"api-only",
	"xml")

gzip_override=0

snooze_routine = conf.get("ZKB","snooze_routine")
query_mod = float(conf.get("ZKB","query_mod"))

class QueryException(Exception):
	def __init__ (self,code):
		self.code = code
	def __str__ (self):
		if self.code == -1:
			return "ZKB requires at least 2 '*ID' identifiers"
		elif self.code == -2:
			mod_list = '\n'.join(valid_modifier)
			return "Invalid query modifier.  Valid Modifiers:\n%s" % mod_list
		elif self.code == -3:
			return "Invalid order modifier.  'asc' or 'desc' only"
		elif self.code == -4:
			return "Invalid ID modifier.  Query limit = %s" % subquery_limit
		elif self.code == -5:
			return "Invalid query modifier type.  Modifier must be base type int"
		else:
			return "Useless generic Exception"
			
class Query(object):
	__initialized = False
	def __init__ (self, startDate, queryArgs=""):
		self.address = base_query
		self.queryArgs = queryArgs
		self.queryElements = {}
		self.IDcount = 0
		self.startDate = startDate
		self.startDatetime = datetime.strptime(self.startDate,"%Y-%m-%d")
		if queryArgs != "":
			self.IDcount +=2
			#do load into queryElements
		self.__initialized == True
		
	def fetch(self):
		return fetchResults(self)
		
	def parseQueryArgs(self,queryArgs):	#Would prefer to also do internal validation
		arg_list = queryArgs.split('/')
		arg_obj = {}
		previous_item = ""
		for item in arg_list:
			if item in valid_modifiers:
				self.queryElements[item] = True
				continue
			split_list = item.split(',')
			if item.isdigit():
				queryElements[previous_item]=item
			elif len(split_list)>1:
				queryElements[previous_item]=item
				
			previous_item = item
			
	def orderDirection(self,dir):
		dirLower = dir.lower()
		if dirLower not in ("asc","desc"):
			raise QueryException(-3)
		
		self.queryElements["orderDirection"] = dirLower
		
	def startTime(self,datevalue):
		validTime = False
		try:
			date = time.strptime(datevalue,"%Y-%m-%d")
		except ValueError as e:
			try:
				date = time.strptime(datevalue,"%Y-%m-%d %H:%M")
			except ValueError as e2:
				try:
					date = time.strptime(datevalue,"%Y%m%d%H%M")
					validTime = True
				except ValueError as e3:
					raise e3
					
		date_str = ""
		if validTime:
			date_str = datevalue
		else:
			date_str = dateConv(date)
		self.queryElements["startTime"] = date_str

	def endTime(self,datevalue):
		validTime = False
		try:
			date = time.strptime(datevalue,"%Y-%m-%d")
		except ValueError as e:
			try:
				date = time.strptime(datevalue,"%Y-%m-%d %H:%M")
			except ValueError as e2:
				try:
					date = time.strptime(datevalue,"%Y%m%d%H%M")
					validTime = True
				except ValueError as e3:
					raise e3
				
		date_str = ""
		if validTime:
			date_str = datevalue
		else:
			date_str = dateConv(date)
			
		self.queryElements["endTime"] = date_str
				
	def dateConv (self,date):
		date_str = date.strftime("%Y%m%d%H%M")
		return date_str
	
	def limit (self,limit):
		if self.singletonValidator(limit):
			self.queryElements["limit"] = limit
		else:
			raise QueryException(-5)
			
	def page (self,page):
		if self.singletonValidator(page):
			self.queryElements["page"] = page
		else:
			raise QueryException(-5)
	
	def year (self,year):
		if self.singletonValidator(year):
			self.queryElements["year"] = year
		else:
			raise QueryException(-5)
			
	def month(self,month):
		if self.singletonValidator(month):
			self.queryElements["month"] = month
		else:
			raise QueryException(-5)
			
	def week (self,week):
		if self.singletonValidator(week):
			self.queryElements["week"] = week
		else:
			raise QueryException(-5)
			
	def beforeKillID (self,killID):
		self.IDcount +=1	
		self.queryElements["beforeKillID"] = self.idValidator(killID)
		
	def afterKillID (self,killID):
		self.IDcount +=1	
		self.queryElements["afterKillID"] = self.idValidator(killID)
		
	def pastSeconds (self,seconds):
		self.IDcount +=1	
		self.queryElements["pastSeconds"] = self.idValidator(seconds)
		
	def characterID (self,characterID):
		self.IDcount +=2	
		self.queryElements["characterID"] = self.idValidator(characterID)
		
	def corpoartionID (self,corporationID):
		self.IDcount +=2	
		self.queryElements["corpoartionID"] = self.idValidator(corporationID)
		
	def allianceID (self,allianceID):
		self.IDcount +=2	
		self.queryElements["allianceID"] = self.idValidator(allianceID)
		
	def factionID (self,factionID):
		self.IDcount +=1	
		self.queryElements["factionID"] = self.idValidator(factionID)
		
	def shipTypeID (self,shipTypeID):
		self.IDcount +=1	
		self.queryElements["shipTypeID"] = self.idValidator(shipTypeID)
		
	def groupID (self,groupID):
		self.IDcount +=1	
		self.queryElements["groupID"] = self.idValidator(groupID)
		
	def solarSystemID (self,solarSystemID):
		self.IDcount +=1
		self.queryElements["solarSystemID"] = self.idValidator(solarSystemID)
		
	def regionID (self,regionID):
		self.IDcount +=1
		self.queryElements["regionID"] = self.idValidator(regionID)
		
	def singletonValidator (self,value):
		valid = False
		if isinstance(value,str):
			if value.isdigit():
				valid = True
		elif isinstance(value,int):
			valid = True
		return valid
		
	def idValidator (self,value):
		returnstr = ""
		if isinstance(value,str):
			tmp_list = value.split(',')
			valid_list = True
			for individual in tmp_list:
				if self.singletonValidator(individual) == False:
					raise QueryException(-5)
					
			returnstr = value
		elif isinstance(value,int):
			returnstr = str(value)
			
		elif type(value) is list:
			valid_list = True
			for individual in value:
				if self.singletonValidator(individual) == False:
					raise QueryException(-5)
					
			returnstr = ','.join(str(x) for x in value)
		return returnstr
		
	def __getattr__ (self,name):	#for modifiers
		mod_str = name.replace('_','-')
		if mod_str not in valid_modifiers:
			raise QueryException(-2)
		
		
		if mod_str in ("w-space","solo"):
			self.IDcount += 1
		
		self.queryElements[str(mod_str)] = True
		
	def __iter__ (self):
		query_results_JSON = []
		try:
			self.queryElements["beforeKillID"]
		except KeyError, E:
			self.queryElements["beforeKillID"] = fetchLatestKillID(self.startDate)
		
		query_complete = False
		while query_complete == False:
			result_JSON = []
			try:
				single_query_JSON = fetchResult(str(self))
			except Exception, E:
				print "Fatal exception, going down in flames"
				print E
				_dump_results(self,query_results_JSON)	#major failure, dump for restart
				sys.exit(3)
			
			beforeKill = int(earliestKillID(single_query_JSON))
			
			if len(single_query_JSON) == 0:
				query_complete = True
				continue
			
			for kill in single_query_JSON:
				if datetime.strptime(kill["killTime"],"%Y-%m-%d %H:%M:%S") > self.startDatetime:
					result_JSON.append(kill)
					query_results_JSON.append(kill)
				else:
					query_complete = True
					
			self.beforeKillID(beforeKill)
			_dump_results(self,query_results_JSON)
			
			yield result_JSON
			
	def __str__ (self):
		if self.IDcount < 2:
			raise QueryException(-1)
		query_modifiers = self.queryArgs
		for key,value in self.queryElements.iteritems():
			if value == True:
				query_modifiers = "%s/%s" % (key,query_modifiers)	#fetch modifiers must be first
			else:
				query_modifiers = "%s%s/%s/" % (query_modifiers,key,value)
				
		return "%s%s" % (self.address,query_modifiers)
	
def latestKillID(kill_obj):
	earliest_time = datetime.strptime("1970-01-01 00:00:00","%Y-%m-%d %H:%M:%S")	#epoch 0
	latest_ID = 0
	for kill in kill_obj:
		killTime = datetime.strptime(kill["killTime"],"%Y-%m-%d %H:%M:%S")
		if killTime > earliest_time:
			earliest_time = killTime
			latest_ID = kill["killID"]
			
	return latest_ID
	
def earliestKillID(kill_obj):
	latest_time = datetime.utcnow()
	earliest_ID = 0
	for kill in kill_obj:
		killTime = datetime.strptime(kill["killTime"],"%Y-%m-%d %H:%M:%S")
		if killTime < latest_time:
			latest_time = killTime
			earliest_ID = kill["killID"]
			
	return earliest_ID

def fetchResults(queryObj,joined_json = []):
	query_complete = False
	
	try:	#only start at latest killID if not already assigned
		beforeKill = queryObj.queryElements["beforeKillID"]
	except KeyError as E:
		beforeKill = fetchLatestKillID(queryObj.startDate)
		queryObj.beforeKillID(beforeKill)
	
	while query_complete == False:
		
		print "fetching: %s" % queryObj
		
		try:
			tmp_JSON = fetchResult(str(queryObj))	#fetch single query result
		except Exception, E:
			print "Fatal exception, going down in flames"
			print E
			_dump_results(queryObj,joined_json)	#major failure, dump for restart
			sys.exit(3)
			
		beforeKill = earliestKillID(tmp_JSON)
		
		if len(tmp_JSON) == 0:	#if return is empty (and valid) complete
			query_complete = True
			continue
			
		for kill in tmp_JSON:
			if datetime.strptime(kill["killTime"],"%Y-%m-%d %H:%M:%S") > queryObj.startDatetime:
				joined_json.append(kill)	#dump all valid entries into object
			else:
				query_complete = True
				
		queryObj.beforeKillID(beforeKill)	#reset the queryObj before dumping
		_dump_results(queryObj,joined_json)
	return joined_json
	
def fetchResult(zkb_url):	
	global sleepTime
	global gzip_override
	
	request = urllib2.Request(zkb_url)
	request.add_header('Accept-Encoding','gzip')
	request.add_header('User-Agent',User_Agent)
	
	#log query
	
	for tries in range (0,retry_limit):
		time.sleep(sleepTime)			#default wait between queries
		time.sleep(default_sleep*tries)	#wait in case of retry
		
		try:
			opener = urllib2.build_opener()	
			raw_zip = opener.open(request)
			http_header = raw_zip.headers
			dump_zip_stream = raw_zip.read()
			#print http_header
			#print sleepTime
		except urllib2.HTTPError as e:
			#log_filehandle.write("%s: %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), e))
			print "retry %s: %s" %(zkb_url,tries+1)
			print raw_zip.headers
			continue
		except urllib2.URLError as er:
			#log_filehandle.write("%s: %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), er))
			print "URLError.  Retry %s: %s" %(zkb_url,tries+1)
			print raw_zip.headers
			continue
		except socket.error as err:
			#log_filehandle.write("%s: %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), err))
			print "Socket Error.  Retry %s: %s" %(zkb_url,tries+1)
		
		if snooze_routine == "HOURLY":
			sleepTime = _hourlySnooze(http_header)
		elif snooze_routine == "POLITE":
			sleepTime = _politeSnooze(http_header)
		else:
			sleepTime = default_sleep
			
		if gzip_override == 0:	#Hard override of gzip encoding
			try:
				dump_IOstream = StringIO.StringIO(dump_zip_stream)
				zipper = gzip.GzipFile(fileobj=dump_IOstream)
				JSON_obj = json.load(zipper)
			except ValueError as errr:
				#log_filehandle.write("%s: %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), errr))
				print "Empty response.  Retry %s: %s" %(zkb_url,tries+1)
			except IOError as errrr:
				print "gzip header broken.  Disabling gzip and retry"
				gzip_override = 1
				fetchResult(zkb_url)
			else:
				break
		else:	#If gzipencoding is broken, treat return as valid JSON
			try:
				JSON_obj = json.loads(dump_zip_stream)
			except ValueError:	#if stream is grossly not-JSON
				print "Unable to read return.  enabling gzip again"
				gzip_override = 0
				fetchResult(zkb_url)
			break
			
	else:
		print http_header
		sys.exit(2)
	
	return JSON_obj

def fetchLatestKillID (start_date):
	singleton_query = Query(start_date,"api-only/solo/kills/limit/1/")
	kill_obj = fetchResult(str(singleton_query))
	
	return kill_obj[0]["killID"]
	
def _snooze(http_header,multiplier=1):
	global query_limit, sleepTime
	
	try:
		query_limit  = int(http_header["X-Bin-Max-Requests"])
		request_used = int(http_header["X-Bin-Request-Count"])
	except KeyError as e:
		sleepTime = query_limit/(24*60*60)*multiplier
		return sleepTime
	sleepTime = 0
	if request_used/query_limit <= 0.5:
		return sleepTime
	elif request_used/query_limit > 0.9:
		sleepTime = query_limit/(24*60*60)*multiplier*2
		return sleepTime	
	elif request_used/query_limit > 0.75:
		sleepTime = query_limit/(24*60*60)*multiplier
		return sleepTime
	elif request_used/query_limit > 0.5:
		sleepTime = query_limit/(24*60*60)*multiplier*0.5
		return sleepTime
	else:
		sleepTime = query_limit/(24*60*60)*multiplier
		return sleepTime
		
def _snoozeSetter(http_header):
	global query_limit,snooze_timer
	
	try:
		query_limit = int(http_header["X-Bin-Max-Requests"])
	except KeyError as e:
		print "WARNING: http_header key 'X-Bin-Max-Requests' not found"
		query_limit = int(conf.get("ZKB","query_limit"))
		snooze_timer = query_limit/(24*60*60)	#requests per day
			
def _politeSnooze(http_header):
	global snooze_timer
	call_sleep = 0
	conn_allowance = int(http_header["X-Bin-Attempts-Allowed"])
	conn_reqs_used = int(http_header["X-Bin-Requests"])	
	conn_sleep_time= int(header["X-Bin-Seconds-Between-Request"])
	
	if (conn_reqs_used+1)==conn_allowance:
		time.sleep(conn_sleep_time)

def _hourlySnooze(http_header):
	#Designed to work with queries/hour rules
	snooze = default_sleep
	progress = 0
	try:
		allowance = int(http_header["X-Bin-Max-Requests"])
		
	except KeyError as e:
		print "WARNING: X-Bin-Max-Requests not defined in header"
		return default_sleep
	try:
		progress = int(http_header["X-Bin-Request-Count"])
	except KeyError as e:
		print "WARNING: X-Bin-Request-Count not defined in header"
	
	snooze = (3600 / allowance) * query_mod
	
	if (progress/allowance) > 0.65:
		snooze * 1.1
	elif (progress/allowance) > 0.80:
		snooze * 1.5
	elif (progress/allowance) > 0.90:
		snooze * 2
		
	return snooze
def _dump_results(queryObj,results_json):
	dump_obj = []
	dump_obj.append(str(queryObj))
	dump_obj.append(queryObj.startDate)
	for kill in results_json:
		dump_obj.append(kill)

	
	dump = open(result_dumpfile,'w')
	dump.write(json.dumps(dump_obj,indent=4))
	dump.close()

def crash_recovery():
	print "recovering from file"
	dump_obj = json.load(open(result_dumpfile))
	query_address = dump_obj.pop(0)
	query_startdate = dump_obj.pop(0)
	
	zkb_args = query_address.split(base_query)
	
	crashQuery = Query(query_startdate,zkb_args)
	
	fetchResults(crashQuery,dump_obj)	#this isn't perfect.  Would prefer higher level control
	
def main():
	newQuery2 = Query("2013-12-20","api-only/corporationID/1894214152/")
	
	#_crash_recovery()
	
	#newQuery.api_only
	#newQuery.characterID(628592330)
	#newQuery.losses
	
	print newQuery2
	
	test_return = newQuery2.fetch()
	print len(test_return)
	
if __name__ == "__main__":
	main()	