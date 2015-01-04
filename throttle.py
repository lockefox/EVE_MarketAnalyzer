from zkb_config import *
import requests

class ProgressManagerBase(object):
	def __init__(self, quota):
		self.quota = quota
	def report(self, elapsed, current=None, quota=None, over_quota=False): pass
	def current_throttle(self): pass

class SimpleProgressManager(ProgressManagerBase):
	def __init__(self, quota):
		ProgressManagerBase.__init__(self, quota)


class ProgressManager(ProgressManagerBase):
	pass

class ThreadedProgressManager(ProgressManagerBase):
	pass

class FlowManager(object):
	def __init__(self, max_tries=retry_limit, progress_obj=None):
		self.max_tries = max_tries
		self.urls = {}
		self.progress = progress_obj or SimpleProgressManager()

	def server_error(self, resp):
		assert(isinstance(resp, requests.Response))
		tries, rest = self.urls.setdefault(resp.request.url, (0, 0.0))
		self.urls[resp.request.url] = (tries + 1, rest)
		if tries + 1 == max_tries or response.status_code == 406:
			# 406 -- your query was bad and you should feel bad (no recovery possible)
			resp.raise_for_status()
		elif response.status_code in (403, 429):
			# You are over quota
			self.update_throttle(resp, emergency_over_quota=True)
		elif response.status_code in (404, 500, 502, 503, 520): 
			# Internal server error -- probably not your fault.
			# I mean -- 404 or 500 probably would be your fault but we checked your query
			# before you issued it so we'll give you the benefit of the doubt.
			self.hard_throttle(resp.request.url, rest + 2.0)
		else: 
			# God only knows -- let's see if someone else can make sense of the problem
			self.update_throttle(resp)

	def transport_exception(self, url, ex):
		assert(isinstance(ex, Exception))
		assert(isinstance(resp, requests.Response))
		tries, rest = self.urls.setdefault(resp.request.url, (0, 0.0))
		# simple linear backoff irrespective of exception type
		self.urls[url] = (tries + 1, rest + 1.0)
		if tries == max_tries:
			raise ex

	def hard_throttle(self, url, seconds):
		tries, rest = self.urls.setdefault(url, (1, 0.0))
		self.urls[url] = (tries, max(rest, seconds))

	def update_throttle(self, resp, emergency_over_quota=False):
		assert(isinstance(resp, requests.Response))
		current = resp.headers.get('x-bin-request-count')
		quota = resp.headers.get('x-bin-max-requests')
		self.progress.report(resp.elapsed, current, quota, emergency_over_quota)
		(tries, rest) = self.urls.setdefault(resp.request.url, (1, 0.0))
		self.urls[resp.request.url] = (tries, max(self.progress.current_throttle(), rest))

	def throttle(self, url):
		_, rest = self.urls.get(url, (0, 0.0))
		time.sleep(rest)
