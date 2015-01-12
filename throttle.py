from __future__ import division
from zkb_config import *
import time, requests, _strptime # because threading
import threading
from Queue import Queue, PriorityQueue, deque, Empty

class ProgressManager(object):
	def __init__(
			self, 
			quota=zkb_scrape_limit, 
			quota_period=zkb_quota_period, 
			tuning_period=zkb_tuning_period
		):
		assert(isinstance(quota, int))
		if quota <= 1: raise ValueError("quota must be at least 1: ", quota)
		self.quota = quota
		self.quota_period = quota_period
		self.tuning_period = tuning_period

		self.avg_elapsed = self.quota_period / self.quota
		self.avg_headroom = self.quota
		self.avg_wait = 0.0
		tuning_samples = max(10, int(tuning_period * self.quota / self.quota_period))
		self.recent_elapsed = deque([], tuning_samples)
		self.recent_headroom = deque([], tuning_samples)
		self.recent_waits = deque([], tuning_samples)
		self.threads = 0

		self.incoming_reports = PriorityQueue()
		self.draining_requests = deque()
		self.blocked_queries = deque()
		self.report_thread = threading.Thread(
			name="ProgressManager report thread",
			target=self.report_thread_routine
		)
		self.report_thread.daemon = True
		self.report_thread.start()

		self.waiting_queries = PriorityQueue()
		self.wait_thread = threading.Thread(
			name="ProgressManager waiting queries thread",
			target = self.wait_thread_routine
		)
		self.wait_thread.daemon = True
		self.wait_thread.start()

	def register(self):
		self.threads = self.threads + 1
	
	def update_avg_headroom(self):
		if not self.recent_headroom: result = self.quota
		else: result = sum(self.recent_headroom) / len(self.recent_headroom)
		self.avg_headroom = result
		return result

	def update_avg_elapsed(self):
		if not self.recent_elapsed: result = self.quota_period / self.quota
		else: result = sum(self.recent_elapsed) / len(self.recent_elapsed)
		self.avg_elapsed = result
		return result

	def update_avg_wait(self):
		if not self.recent_waits: result = 0.0
		else: result = sum(self.recent_waits) / len(self.recent_waits)
		self.avg_wait = result
		return result

	def update_avgs(self):
		self.update_avg_elapsed()
		self.update_avg_headroom()
		self.update_avg_wait()

	@property
	def optimal_threads(self):
		now = time.time()
		next = now if not self.draining_requests else self.draining_requests[-1]
		time_remaining = self.quota_period + next - now
		return self.avg_elapsed * self.avg_headroom / time_remaining

	def optimal_wait(self, now, headroom, elapsed, use_avg=True):
		elapsed = self.avg_elapsed if use_avg else elapsed
		time_remaining = self.draining_requests[-1] + self.quota_period - now
		result = self.threads * time_remaining / headroom - elapsed
		print "headroom: {0}; quota: {1}; time remaining: {2:.5}; elapsed: {3:.4} optimal wait: {4:.4}; avg wait: {5:.4}; threads: {6}; optimal threads: {7:.4}".format(
				headroom,
				self.quota,
				time_remaining,
				elapsed,
				result,
				self.avg_wait,
				self.threads,
				self.optimal_threads
			)
		return result

	def request_wait(self, event, seconds):
		if seconds > 0:
			event.clear()
			self.waiting_queries.put((time.time() + seconds, event))
		else:
			event.set()

	def wait_thread_routine(self):
		while True:
			wake_time, event = self.waiting_queries.get()
			sleep_time = wake_time - time.time()
			if sleep_time > 0:
				time.sleep(sleep_time)
			event.set()
			self.waiting_queries.task_done()

	def report(self, *args):
		event = args[-1]
		event.clear()
		self.incoming_reports.put(args)

	def hard_block(self, event):
		event.clear()
		self.blocked_queries.appendleft(event)

	def report_thread_routine(self):
		while True:
			try:
				args = self.incoming_reports.get(True, self.avg_elapsed)
				self.do_report(*args)
				self.update_avgs()
				self.incoming_reports.task_done()
			except Empty:
				self.drain_requests(time.time(), 0)

	def drain_requests(self, now, requests_queued):
		# clear any expired requests
		if self.draining_requests:
			w = self.draining_requests.pop()
			while True:
				if now - w > self.quota_period:
					if self.draining_requests:
						w = self.draining_requests.pop()
					else:
						break
				else:
					self.draining_requests.append(w)
					break
		free = self.quota - len(self.draining_requests) - requests_queued
		if free > 0:
			for _ in range(min(free, len(self.blocked_queries))):
				self.blocked_queries.pop().set()

	def rationalize_draining_queue(self, current, now, requests_queued):
		requests_draining = len(self.draining_requests)
		requests_out = requests_draining + requests_queued
		
		# We want to fix up the queue length if there are outstanding requests
		# from a previous session.
		if (current > requests_out):
			print "current: {0}, quota: {1}, draining: {2}".format( 
				current, 
				self.quota,
				requests_out
			)
			for _ in range(current - requests_out):
				self.draining_requests.appendleft(now)
				
		# Clear out any requests that have expired
		self.drain_requests(now, requests_queued)
		requests_draining = len(self.draining_requests)

		# If requests from a previous session have expired we want to clear the 
		# queue but we don't want to be too aggressive in case we get reports out of
		# order (so we won't clear stuff that expires in 1 minute or less).
		if 0 < current < requests_draining:
			w = self.draining_requests.pop()
			if now - w > 60:
				print "!!! draining %s old requests" % (requests_draining - current)
				for _ in range(requests_draining - current):
					self.draining_requests.pop()
			else:
				self.draining_requests.append(w)

	def do_report(self, current, quota, elapsed, over_quota, event):
		# These can change underneath us, so for sake of being able to reason
		# about them, freeze their values.
		now = time.time()
		requests_queued = self.incoming_reports.qsize()

		if not self.draining_requests and over_quota:
			# This can only happen if we used up all our quota before we even started
			# this query session
			raise Exception(
				"Augh! no requests draining, yet over quota? " + 
					"Try again in an hour!"
				)

		# If someone changed the quota on us, fix it up
		if (0 < quota < self.quota or 
			(quota > self.quota and not over_quota)):
			self.quota = quota
		
		# Record the current request
		self.draining_requests.appendleft(now)
		self.recent_elapsed.append(elapsed)
		
		# Fix up the draining queue so it matches what the server thinks
		# (as best as possible)
		self.rationalize_draining_queue(current, now, requests_queued)

		# Calculate the wait and queue it
		requests_out = len(self.draining_requests) + requests_queued
		headroom = self.quota - max(requests_out, current)
		self.recent_headroom.append(headroom)

		if over_quota or headroom <= 0:
			# This can happen if something drastic is different between 
			# the server and our record keeping, or if we've got a lot of 
			# threads and they are processing faster than average.
			# Wait for the excess requests to drain.
			self.hard_block(event)
		else:
			w = self.optimal_wait(now, headroom, elapsed)
			self.recent_waits.append(w)
			self.request_wait(event, w)
		
class FlowManager(object):
	def __init__(self, max_tries=retry_limit, progress_obj=None):
		self.max_tries = max_tries
		self.urls = {}
		self.throttle_event = threading.Event()
		self.progress = progress_obj or ProgressManager()
		self.progress.register()

	def update_throttle(self, resp, emergency_over_quota=False):
		assert(isinstance(resp, requests.Response))
		current = int(resp.headers.get('x-bin-request-count', '0'))
		quota = int(resp.headers.get('x-bin-max-requests', '0'))
		self.progress.report(
			current, 
			quota, 
			resp.elapsed.total_seconds(), 
			emergency_over_quota,
			self.throttle_event
		)

	def hard_throttle(self, url, seconds):
		tries, rest = self.urls.setdefault(url, (1, 0.0))
		self.urls[url] = (tries, max(rest, seconds))
		self.progress.request_wait(self.throttle_event, max(rest, seconds))

	def throttle(self):
		self.throttle_event.wait()

	def server_error(self, resp):
		assert(isinstance(resp, requests.Response))
		tries, rest = self.urls.setdefault(resp.request.url, (0, 0.0))
		self.urls[resp.request.url] = (tries + 1, rest)
		if tries + 1 == self.max_tries or resp.status_code in (406, 409):
			# 406 -- your query was bad and you should feel bad (no recovery possible)
			# 409 -- you asked for pages > 10 and you should feel bad
			resp.raise_for_status()
		elif resp.status_code in (403, 429) or \
				'retry-after' in resp.headers:
			# You are over quota
			self.update_throttle(resp, emergency_over_quota=True)
		elif resp.status_code in (404, 500, 502, 503, 520): 
			# Internal server error -- probably not your fault.
			# I mean -- 404 or 500 probably would be your fault but we checked your query
			# before you issued it so we'll give you the benefit of the doubt.
			self.hard_throttle(resp.request.url, rest + 2.0)
		else: 
			# God only knows -- let's see if someone else can make sense of the problem
			self.update_throttle(resp)
		raise ZkbServerException(resp.status_code)

	def transport_exception(self, url, ex):
		assert(isinstance(ex, Exception))
		tries, rest = self.urls.setdefault(url, (0, 0.0))
		# simple linear backoff irrespective of exception type
		self.urls[url] = (tries + 1, rest)
		if tries == self.max_tries:
			raise ex
		self.hard_throttle(url, rest + 2.0)
