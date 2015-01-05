from __future__ import division
import itertools, time, ConfigParser
from zkb_exceptions import *
flatten = itertools.chain.from_iterable

conf = ConfigParser.ConfigParser()
conf.read(["init.ini", "init_local.ini"])

zkb_base_query = conf.get("ZKB","base_query")
query_limit = int(conf.get("ZKB","query_limit"))
zkb_scrape_limit = int(conf.get("ZKB", "zkb_scrape_limit"))
zkb_quota_period = int(conf.get("ZKB", "zkb_quota_period"))
subquery_limit = int(conf.get("ZKB","subquery_limit"))
retry_limit = int(conf.get("ZKB","retry_limit"))
default_sleep = int(conf.get("ZKB","default_sleep"))
User_Agent = conf.get("GLOBALS","user_agent")
logfile = conf.get("ZKB","logfile")
result_dumpfile = conf.get("ZKB","result_dumpfile")

sleepTime = query_limit/(24*60*60)

snooze_routine = conf.get("ZKB","snooze_routine")
query_mod = float(conf.get("ZKB","query_mod"))

acceptable_date_formats = ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y%m%d%H%M")
def dateValidator(value, format="%Y%m%d%H%M"):
	if value is None: return None
	for f in acceptable_date_formats:
		try:
			date = time.strptime(value, f)
		except ValueError:
			pass
		else:
			return time.strftime(format, date)
	raise InvalidDateFormat(value, acceptable_date_formats)

def singletonValidator(value):
	if value is None: return None
	if (isinstance(value, str) and value.isdigit()) or isinstance(value, int):
		return str(value)
	raise InvalidQueryValue(value, 'Value must be int or convertible to int.')

def idValidator(value):
	if value is None: return None
	if isinstance(value, int):
		vlist = [value]
	elif isinstance(value, str):
		vlist = value.split(',')
	elif isinstance(value, list):
		vlist = value
	if len(vlist) > subquery_limit: raise TooManyIDsRequested(value, subquery_limit)
	return ','.join(vv for vv in [singletonValidator(v) for v in vlist] if vv is not None)

def modValidator(value): return True if value else None

def orderValidator(value):
	if value is None: return None
	value = value.lower()
	if value in ('asc', 'desc'):
		return value
	raise InvalidQueryValue(value, "must be 'asc' or 'desc'")

def secondsValidator(value):
	if value is None: return None
	secs = int(value)
	if secs <= (86400 * 7): 
		return str(secs)
	raise InvalidQueryValue(value, "pastSeconds is limited to a max of 7 days.")

def pageValidator(value):
	if value is None: return None
	pages = int(value)
	if pages <= 10: 
		return str(secs)
	raise InvalidQueryValue(value, "page is usually limited to a max of 10.")


zkb_required = (
	"characterID", 
	"corporationID", 
	"allianceID", 
	"factionID", 
	"shipTypeID", 
	"groupID", 
	"solarSystemID", 
	"solo", 
	"w-space", 
	"warID", 
	"killID"
)

zkb_modifiers = (
	"kills",
	"losses",
	"w-space",
	"solo",
	"no-items",
	"no-attackers",
	"api-only",
	"xml",
	"pretty",
	"finalblow-only"
)

zkb_date_params = (
	"startTime",
	"endTime"
)

zkb_singleton_params = (
	'limit',
	'year',
	'month',
	'week',
	'beforeKillID',
	'afterKillID',
	'killID',
	'warID',
	'iskValue'
)

zkb_id_params = (
	'characterID',
	'corporationID',
	'allianceID',
	'factionID',
	'shipTypeID',
	'groupID',
	'solarSystemID',
	'regionID'
)

zkb_unique_params = (
	('orderDirection', orderValidator),
	('pastSeconds', secondsValidator),
	('page', pageValidator)
)

zkb_params = (
	(zkb_modifiers, modValidator),
	(zkb_date_params, dateValidator),
	(zkb_singleton_params, singletonValidator),
	(zkb_id_params, idValidator)
)

zkb_synonyms = { 
	'ship': 'shipTypeID',
	'shipID': 'shipTypeID',
	'shipType': 'shipTypeID',
	'character': 'characterID',
	'corporation': 'corporationID',
	'alliance': 'allianceID',
	'faction': 'factionID',
	'group': 'groupID',
	'system': 'solarSystemID',
	'systemID': 'solarSystemID',
	'solarSystem': 'solarSystemID',
	'region': 'regionID',
	'startDate': 'startTime',
	'endDate': 'endTime',
	'war': 'warID'
}

zkb_rest_parameters = dict(
	itertools.chain(
		flatten(zip(p, itertools.repeat(v)) for p, v in zkb_params), 
		zkb_unique_params
	)
)
