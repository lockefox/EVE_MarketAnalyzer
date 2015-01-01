from zkb_config import *

class RecoveryPolicy(object):
	def __init__(self, max_tries=retry_limit):
		self.max_tries = max_tries
		self.uris = {}

	def server_error(self, resp):
		tries = self.uris.setdefault(resp.request.url, 0)
		self.uris[resp.request.url] = tries + 1
		def bail_out():
			resp.raise_for_status()
			return True
		if tries == max_tries or response.status_code in (404, 406, 500):
			return bail_out
		elif response.status_code in (403, 429): pass
			# You are over quota
			# Look at the retry-after header
		elif response.status_code in (502, 503, 520): pass
			# Internal server error -- probably not your fault.
		else: pass
			# see if you can make sense of retry-after, x-bin-request-count, or x-bin-max-requests headers.
		return lambda: True

	def transport_exception(self, uri, ex):
		tries = self.uris.setdefault(resp.request.url, 0)
		self.uris[uri] = tries + 1
		def bail_out():
			raise ex
			return True
		if tries == max_tries:
			return bail_out
		return lambda: True

class FatalisticPolicy(RecoveryPolicy):
	pass

class TenaciousPolicy(RecoveryPolicy):
	pass

class ThrottlePolicy(object):
	def __init__(self):
		self.uris = {}
	def update_throttle(self, uri, used, quota):
		self.uris[uri] = (used, quota)
	def throttle(self, uri):
		pass

class HourlySnooze(ThrottlePolicy):
	pass

class Hour2Snooze(ThrottlePolicy):
	pass

class PoliteSnooze(ThrottlePolicy):
	pass

class ThreadedThrottle(ThrottlePolicy):
	pass

class RetryPolicy(object):
	def __init__(self, recovery_policy=None, throttle_policy=None):
		self.recovery_policy = recovery_policy or RecoveryPolicy()
		self.throttle_policy = throttle_policy or ThrottlePolicy()

	def server_error(self, resp):
		return self.recovery_policy.server_error(resp)

	def transport_exception(self, uri, ex):
		return self.recovery_policy.transport_exception(uri, ex)

	def update_throttle(self, uri, used, quota):
		return self.throttle_policy.update_throttle(uri, used, quota)

	def throttle(self, uri):
		return self.throttle_policy.throttle(uri)

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

def _hour2Snooze(http_header):
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
	
	if (progress/allowance) > 0.65:
		if snooze == 0: snooze += 1
		snooze * 2
	elif (progress/allowance) > 0.80:
		if snooze == 0: snooze += 1
		snooze * 4
	elif (progress/allowance) > 0.90:
		if snooze == 0: snooze += 1
		snooze * 8
	
	if (allowance - progress) <= 10:	#emergency backoff
		print 'critical allowance: %s' % (allowance - progress)
		snooze = (3600/allowance) * 10
	return snooze	

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
	print 3600 / allowance
	print '%s:%s' % (progress, allowance)
	snooze = (3600 / allowance) * query_mod
	
	if (progress/allowance) > 0.65:
		snooze * 1.1
	elif (progress/allowance) > 0.80:
		snooze * 1.5
	elif (progress/allowance) > 0.90:
		snooze * 2
		
	return snooze
