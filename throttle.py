from __future__ import division
from zkb_config import *
import time, requests, _strptime # because threading
import threading
from Queue import PriorityQueue, deque, Empty

class ProgressManagerBase(object):
	def __init__(self, quota=zkb_scrape_limit):
		self.quota = quota
	def report(self, current, quota, elapsed, over_quota=False): pass
	def current_throttle(self): pass
	def headroom(self): pass

class SimpleProgressManager(ProgressManagerBase):
	def __init__(self, quota=zkb_scrape_limit):
		ProgressManagerBase.__init__(self, quota)
		self.draining_requests = deque()
		self.recent_elapsed = deque([], 30)
		self.next_throttle = 0.0
		self.bolus = 0

	def drain_requests(self, now):
		# clear any expired requests
		if self.draining_requests:
			w = self.draining_requests.pop()
			while True:
				if now - w > 3600:
					if self.draining_requests:
						w = self.draining_requests.pop()
					else:
						break
				else:
					self.draining_requests.append(w)
					break



	def report(self, current, quota, elapsed, over_quota=False):
		if not self.draining_requests and over_quota:
				# This can only happen if we used up all our quota before we even started
				# this query session
				raise Exception(
					"Augh! no requests draining, yet over quota? " + 
						"Try again in an hour!"
					)

		now = time.time()
		requests_out = len(self.draining_requests)

		# We want to fix up the queue length if there are outstanding requests
		# from a previous session, but we don't want to double-count if we
		# get reports out of order.
		# In the normal case current == requests_out + 1
		if (current > requests_out):
			if current - requests_out > 1:
				print "current: {0}, quota: {1}, draining: {2}".format( 
					current, 
					quota, 
					requests_out)
			for _ in range(current - requests_out):
				self.draining_requests.appendleft(now)
		# If requests from a previous session have expired we want to clear the 
		# queue but we don't want to be too aggressive in case we get reports out of
		# order (so we won't clear stuff that expires in 5 minutes or less).
		elif 0 < current < requests_out:
			w = self.draining_requests.pop()
			if now - w > 300:
				for _ in range(requests_out - current):
					self.draining_requests.pop()
			else:
				self.draining_requests.append(w)

		self.drain_requests(now)

		self.recent_elapsed.append(elapsed)

		requests_out = len(self.draining_requests)

		if (0 < quota < self.quota or 
			(quota > self.quota and not over_quota)):
			self.quota = quota

		if requests_out == 0:
			# should never happen
			self.next_throttle = 0.0
			return
		elif over_quota:
			# This can only happen if something drastic is different between 
			# the server and our record keeping. 
			excess = max(current - quota + 2, abs(min(self.headroom(), 0)) + 2, 100)
			frac_used = 1.0
			to_drain = min(requests_out, excess)
			avg_r = 0.0
		else:
			frac_used = requests_out / self.quota
			one = (self.quota - 1) / self.quota
			if frac_used < 0.5: frac_used = 0.0
			elif frac_used < 0.875: frac_used = 8 * (frac_used - 0.5) / 3
			elif frac_used < 1.0: frac_used = 1.0
			if requests_out < self.quota:
				frac_used = frac_used * one
				avg_r = self.average_response()
			else:
				avg_r = 0.0
			# if we are over quota but didn't get an over quota error, that's
			# weird but we'll deal with it here.
			to_drain = abs(self.headroom()) if self.headroom() < 0 else 1

		self.draining_requests.rotate(to_drain-1)
		w = self.draining_requests.pop()
		self.next_throttle = max(frac_used * (3600 + w - now) - avg_r, 0.0)
		self.draining_requests.append(w)
		self.draining_requests.rotate(1-to_drain)

	def headroom(self):
		return self.quota - len(self.draining_requests)

	def average_response(self):
		if not self.recent_elapsed: return 2.0
		return sum(self.recent_elapsed) / len(self.recent_elapsed)

	def current_throttle(self):
		return self.next_throttle

class ProgressManager(SimpleProgressManager):
	pass

class ThreadedProgressManager(ProgressManager):
	def __init__(self, quota=zkb_scrape_limit):
		ProgressManager.__init__(self, quota)
		self.incoming_reports = Queue()
		self.report_thread = threading.Thread(
			name="ProgressManager report thread",
			target=self.report_thread_routine
		)
		self.report_thread.daemon = True
		self.report_thread.start()

	def report_thread_routine(self):
		while True:
			try:
				args = self.incoming_reports.get(True, self.average_response())
				ProgressManager.report(self, *args)
				self.incoming_reports.task_done()
			except Queue.Empty:
				self.drain_requests(time.time())

	def report(self, *args):
		self.incoming_reports.put(args)
		
class FlowManager(object):
	def __init__(self, max_tries=retry_limit, progress_obj=None):
		self.max_tries = max_tries
		self.urls = {}
		self.progress = progress_obj or ThreadedProgressManager()

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
		current = int(resp.headers.get('x-bin-request-count', '0'))
		quota = int(resp.headers.get('x-bin-max-requests', '0'))
		if ((current > quota > 0) or
			resp.status_code in (403, 429) or 
			resp.headers.get('retry-after') is not None):
			emergency_over_quota = True
		self.progress.report(current, quota, resp.elapsed, emergency_over_quota)
		(tries, rest) = self.urls.setdefault(resp.request.url, (1, 0.0))
		self.urls[resp.request.url] = (tries, max(self.progress.current_throttle(), rest))

	def throttle(self, url):
		_, rest = self.urls.get(url, (0, 0.0))
		time.sleep(rest)
