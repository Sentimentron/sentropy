#!/usr/bin/env python

# Crawl Processor 
import logging
import os
import threading
from lxml import etree

from pysen.documents import DocumentClassifier

class CrawlProcessor(object):

	def __init__(self, ):

		self.dc = DocumentClassifier()

	def process_record(self, record):

		headers, content, url, date_crawled, content_type = [str(i) for i in record]
		if content_type != 'text/html':
			logging.error("Unsupported content type: %s", str(row))
			return False 
		logging.debug(content)
		logging.debug(url)
		# Start the async transaction to get the plain text
		worker_req_thread = BoilerPipeWorker(content)
		worker_req_thread.start()

		# Whilst that's executing, parse the document 

		# Extract the dates 

		# Wait for the BoilerPipe thread to complete
		worker_req_thread.join()
		logging.debug(worker_req_thread.result)
		logging.debug(worker_req_thread.version)

		if worker_req_thread.result == None:
			return 0
		return len(worker_req_thread.result)

		# Run keyword extraction 

		# Run sentiment analysis

		# Build database objects 

		# Commit to database, return True on success


class BoilerPipeWorker(threading.Thread):

	def __init__(self, body_text):
		self.orig = body_text 
		self.result = None 
		self.version = None
		threading.Thread.__init__(self)

	def run(self):
		import requests
		post = {"charset": "UTF-8", "content": self.orig, "method":"default"}
		r = requests.post(os.environ["BOILERPIPE_URL"], data = post)
		r.raise_for_status()

		try:
			parsed  = etree.fromstring(r.text.encode('ascii'))
		except Exception as ex:
			logging.critical("Failed to parse: %s (%s)", r.text, ex)
			return 

		server_node = parsed.find("ServerInfo")
		if server_node is None:
			raise ValueError("No ServerInfo")

		self.version = "Unknown"
		version = server_node.find("Version")
		if version is None:
			raise ValueError("Couldn't find version information: %s" % (r.text,))
		self.version = version.text

		logging.debug(parsed)
		if parsed.find("ExtractionFailureResponse") is not None:
			return None

		content = parsed.find("Response")
		if content is None:
			raise ValueError(("Couldn't find response", r.text))

		self.result = content.text 