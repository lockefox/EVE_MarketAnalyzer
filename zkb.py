#!/Python27/python.exe
from __future__ import division
import json, requests, _strptime # because threading
from datetime import datetime
from zkb_config import *
from throttle import *

_strptime.IGNORECASE

try:
#for live connections later
	import stomp
	CANSTOMP = True
except ImportError:
	CANSTOMP = False

log = open (logfile, 'a+')

class ZKBQueryBuilder(object):
	Base = zkb_base_query # the URI root
	Synonyms = zkb_synonyms # a dict of 'alternateName': 'canonicalName' pairs
	Parameters = zkb_rest_parameters # a dict of 'parameterName': validatorFunction pairs
	Modifiers = zkb_modifiers # a list of query parameters that don't take a value
	Required = zkb_required # a list of query parameters which you must have at least one of (not all are required)

	def __init__(self, queryArgs=""):
		self.queryElements = {}
		self.queryModifiers = set()
		self.parseQueryArgs(queryArgs)

	def reset(self):
		self.queryElements = {}
		self.queryModifiers = set()

	def parseQueryArgs(self, queryArgs):
		param = None
		for item in queryArgs.split('/'):
			if param is not None:
				self.validateAndSet(param, item)
				param = None
			elif item in self.Modifiers:
				self.validateAndSet(item, True)
			elif item:
				param = item
	
		
	def validateAndSet(self, name="", value=None):
		name = name.replace('_', '-')
		if name in self.Synonyms: name = self.Synonyms[name]
		if name not in self.Parameters:
			raise InvalidQueryParameter(
				name, 
				sorted(
					self.Parameters.keys() + 
					self.Synonyms.keys()
				)
			)
		validator = self.Parameters[name]
		value = validator(value)
		if name in self.Modifiers:
			if value:
				self.queryModifiers.add(name)
			elif name in self.queryModifiers:
				self.queryModifiers.remove(name)
		else:
			if value is not None:
				self.queryElements[name] = value
			elif name in self.queryElements:
				self.queryElements.pop(name)
		return self

	def __getattr__(self, name):
		if name.startswith("_"):
			return object.__getattr__(self, name)
		qname = name.replace('_', '-')
		if qname not in self.Synonyms and qname not in self.Parameters:
			raise InvalidQueryParameter(
				name, 
				sorted(
					self.Parameters.keys() + 
					self.Synonyms.keys()
				)
			)
		return lambda v=True: self.validateAndSet(name=name, value=v)

	def getQueryArgs(self):
		query_args = sorted(self.queryModifiers)
		query_args += sorted(
			"{0}/{1}".format(p, v) 
				for p, v in self.queryElements.iteritems() 
				if v is not None
			)
		return "/".join(query_args)
		
	def __str__(self):
		return self.Base + "/" + self.getQueryArgs() + "/"

	def is_valid(self):
		params = set(self.queryElements.keys()) | self.queryModifiers
		required = set(self.Required)
		return len(params & required) > 0

	def get_query(self):
		if self.is_valid():
			return str(self)
		raise TooFewRequiredParameters(str(self), self.Required)

class ZKBQuery(ZKBQueryBuilder):		
	def __init__ (self, startDate, queryArgs="", manager=None):
		ZKBQueryBuilder.__init__(self, queryArgs)
		self.policy = manager or FlowManager()
		self.startDate = dateValidator(startDate, "%Y-%m-%d")
		self.startDateTime = datetime.strptime(self.startDate,"%Y-%m-%d")
		self.startTime(self.startDate)

	def __iter__(self):
		while True:
			# no try/except -- it is the responsibility of the caller to handle recovery
			single_query_JSON = self.fetch_one()
			if len(single_query_JSON) == 0: break

			beforeKillTime, beforeKillID = earliestKill(single_query_JSON)
			print beforeKillTime.strftime("%Y-%m-%d %H:%M"), beforeKillID
			# result_JSON should be == single_query_JSON
			result_JSON = filter(
				lambda kill: killDateTime(kill) > self.startDateTime, 
				single_query_JSON
			)
			if len(result_JSON) == 0: break

			self.beforeKillID(beforeKillID)
			yield result_JSON

			if beforeKillTime < self.startDateTime: break
			self.policy.throttle()
		
	def fetch(self):
		return fetchResults(self)
	
	def fetch_one(self):
		zkb_url = self.get_query()
		while True:
			try:
				response = requests.get(zkb_url, headers={'User-Agent': User_Agent})
				if not response.ok:
					self.policy.server_error(response)
				else:
					# could raise ValueError if json is bad
					response_json = response.json() 
			except requests.HTTPError:
				# this came from server_error.
				raise
			except Exception as e:
				self.policy.transport_exception(zkb_url, e)
			else:
				self.policy.update_throttle(response)
				return response_json

			self.policy.throttle()

def killDateTime(kill):
	return datetime.strptime(kill["killTime"],"%Y-%m-%d %H:%M:%S")
	
def latestKill(kill_list):
	return max( 
		(killDateTime(kill), int(kill["killID"])) 
			for kill in kill_list
	)

def earliestKill(kill_list):
	return min( 
		(killDateTime(kill), int(kill["killID"])) 
			for kill in kill_list
	)

def fetchResults(queryObj, joined_json = []):
	try:
		for result in queryObj:
			# print queryObj
			joined_json += result
	except:
		print "Fatal exception, going down in flames"
		_dump_results(queryObj, joined_json)	#major failure, dump for restart
		raise
	return joined_json

def fetchLatestKillID(start_date):
	kill_obj = ZKBQuery(start_date, "api-only/solo/kills/limit/1/").fetch_one()
	return int(kill_obj[0]["killID"])
	
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
	
	zkb_args = query_address.split(zkb_base_query)
	
	crashQuery = ZKBQuery(query_startdate, zkb_args)
	
	fetchResults(crashQuery,dump_obj)	#this isn't perfect.  Would prefer higher level control
	
def main():
	newQuery2 = ZKBQuery("2013-01-01","api-only/group/25/")
	
	#_crash_recovery()
	
	#newQuery.api_only
	#newQuery.characterID(628592330)
	#newQuery.losses
	
	print newQuery2
	total = 0
	for result in newQuery2:
		total += len(result)
	#print newQuery2.queryElements["beforeKillID"]
	# test_return = newQuery2.fetch()
	print total
	
if __name__ == "__main__":
	main()	