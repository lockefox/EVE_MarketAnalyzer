class RecoveryPolicy(object):
	def __init__(self, max_tries=10):
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
		return lambda: True

class FatalisticPolicy(RecoveryPolicy):
	pass

class TenaciousPolicy(RecoveryPolicy):
	pass

class ThrottlePolicy(object):
	def throttle(self, uri, used, quota):
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
	def __init__(self, Recovery=RecoveryPolicy, Throttle=ThrottlePolicy):
		self.recovery_policy = Recovery()
		self.throttle_policy = Throttle()

	def server_error(self, resp):
		return self.recovery_policy.server_error(resp)

	def transport_exception(self, uri, ex):
		return self.recovery_policy.transport_exception(uri, ex)

	def throttle(self, uri, used, quota):
		return self.throttle_policy(uri, used, quota)