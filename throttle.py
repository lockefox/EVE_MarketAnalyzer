from zkb_config import *
import time, requests, _strptime # because threading
from Queue import Queue, deque

class ProgressManagerBase(object):
	def __init__(self, quota=zkb_scrape_limit):
		self.quota = quota
	def report(self, elapsed, current=None, quota=None, over_quota=False): pass
	def current_throttle(self): pass

class SimpleProgressManager(ProgressManagerBase):
	def __init__(self, quota=zkb_scrape_limit):
		ProgressManagerBase.__init__(self, quota)
		self.draining_requests = deque()
		self.next_throttle = 0.0

	def report(self, elapsed, current=None, quota=None, over_quota=False):
		now = time.time()
		self.draining_requests.appendleft(now)
		if (quota is not None and 
			(quota < self.quota or 
			(quota > self.quota and not over_quota))):
			self.quota = quota
		if (current is not None and current > len(self.draining_requests)):
			for _ in range(current - len(self.draining_requests)):
				self.draining_requests.appendleft(now)
		if over_quota:
			# This can only happen if something drastic is different between 
			# the server and our record keeping. 
			if not len(self.draining_requests):
				raise Exception("Augh! no requests draining, no current requests in the header, yet over quota??!")
			# wait for 100 requests to drain.
			to_drain = min(len(self.draining_requests, 100))
			self.draining_requests.rotate(to_drain-1)
			w = self.draining_requests.pop()
			self.next_throttle = 3600 + w - now
			self.draining_requests.append(w)
			self.draining_requests.rotate(1-to_drain)
		elif not len(self.draining_requests):
			self.next_throttle = 0.0
		elif 0 < len(self.draining_requests) < self.quota-1:
			w = self.draining_requests.pop()
			while True:
				if now - w > 3600:
					if len(self.draining_requests):
						w = self.draining_requests.pop()
					else:
						break
				else:
					self.draining_requests.append(w)
					break
			self.next_throttle = 0.0
		else:
			# wait until the oldest request has drained
			w = self.draining_requests.pop()
			self.next_throttle = 3600 + w - now

	def current_throttle(self):
		return self.next_throttle

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
		self.urls[url] = (tries + 1, rest + 2.0)
		if tries == max_tries:
			raise ex

	def hard_throttle(self, url, seconds):
		tries, rest = self.urls.setdefault(url, (1, 0.0))
		self.urls[url] = (tries, max(rest, seconds))

	def update_throttle(self, resp, emergency_over_quota=False):
		assert(isinstance(resp, requests.Response))
		current = resp.headers.get('x-bin-request-count')
		quota = resp.headers.get('x-bin-max-requests')
		if current is not None: current = int(current)
		if quota is not None: quota = int(quota)
		if ((current is not None and quota is not None and current >= quota) or
			resp.headers.get('retry-after') is not None):
			emergency_over_quota = True
		self.progress.report(resp.elapsed, current, quota, emergency_over_quota)
		(tries, rest) = self.urls.setdefault(resp.request.url, (1, 0.0))
		self.urls[resp.request.url] = (tries, max(self.progress.current_throttle(), rest))

	def throttle(self, url):
		_, rest = self.urls.get(url, (0, 0.0))
		time.sleep(rest)
