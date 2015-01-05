from __future__ import division
from zkb_config import *
import time, requests, _strptime # because threading
import threading
from Queue import Queue, PriorityQueue, deque, Empty

class ProgressManager(object):
	def __init__(self, quota=zkb_scrape_limit):
		self.quota = quota
		self.recent_elapsed = deque([], 30)

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
		self.incoming_reports.put(args)

	def hard_block(self, event):
		event.clear()
		self.blocked_queries.appendleft(event)

	def report_thread_routine(self):
		while True:
			try:
				args = self.incoming_reports.get(True, self.average_response())
				self.do_report(self, *args)
				self.incoming_reports.task_done()
			except Empty:
				self.drain_requests(time.time(), 0)

	def drain_requests(self, now, requests_queued):
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
		free = self.quota - len(self.draining_requests) - requests_queued
		if free > 0:
			for _ in range(min(free, len(self.blocked_queries))):
				self.blocked_queries.pop().set()

	def get_wait_scale(self, requests_out):
		frac_used = requests_out / self.quota
		if frac_used >= 1.0:
			avg_r = 0.0
		else:
			one = (self.quota - 1) / self.quota
			if frac_used < 0.5: frac_used = 0.0
			elif frac_used < 0.875: frac_used = 8 * (frac_used - 0.5) / 3
			elif frac_used < 1.0: frac_used = 1.0
			frac_used = frac_used * one
			avg_r = self.average_response()
		return frac_used, avg_r

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
		requests_draining = len(self.draining_requests)
		requests_out = requests_draining + requests_queued
		headroom = self.quota - max(requests_out, current)

		if requests_out == 0:
			# there should be no way for this to happen.
			self.request_wait(event, 0.0)
			return
		
		if over_quota or headroom <= 0:
			# This can happen if something drastic is different between 
			# the server and our record keeping. 
			# Wait for the excess requests to drain.
			self.hard_block(event)
			return

		frac_used, avg_r = self.get_wait_scale(requests_out)

		w = self.draining_requests[-1]
		self.request_wait(event, frac_used * (3600 + w - now) - avg_r)

	def average_response(self):
		if not self.recent_elapsed: return 2.0
		return sum(self.recent_elapsed) / len(self.recent_elapsed)
		
class FlowManager(object):
	def __init__(self, max_tries=retry_limit, progress_obj=None):
		self.max_tries = max_tries
		self.urls = {}
		self.throttle_event = threading.Event()
		self.progress = progress_obj or ProgressManager()

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
		if not self.throttle_event.is_set():
			print "Waiting."
			self.throttle_event.wait()
			print "Done waiting."

	def server_error(self, resp):
		assert(isinstance(resp, requests.Response))
		tries, rest = self.urls.setdefault(resp.request.url, (0, 0.0))
		self.urls[resp.request.url] = (tries + 1, rest)
		if tries + 1 == max_tries or response.status_code == 406:
			# 406 -- your query was bad and you should feel bad (no recovery possible)
			resp.raise_for_status()
		elif response.status_code in (403, 429) or \
				response.headers.has_key('retry-after'):
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
		self.urls[url] = (tries + 1, rest)
		if tries == max_tries:
			raise ex
		self.hard_throttle(url, rest + 2.0)
