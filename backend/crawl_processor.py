#!/usr/bin/env python

# Crawl Processor 
import logging
import os
import threading

from lxml import etree
from bs4 import BeautifulSoup

from pysen.documents import DocumentClassifier
import pydate

from db import Article, Domain, DomainController, ArticleController

class CrawlProcessor(object):

	def __init__(self, engine):

		self.cls = DocumentClassifier()
		self.dc  = DomainController(engine)
		self.ac  = ArticleController(engine)

	def process_record(self, item):

		crawl_id, record = item

		headers, content, url, date_crawled, content_type = [str(i) for i in record]
		status = "Processed"

		try:
			if content_type != 'text/html':
				logging.error("Unsupported content type: %s", str(row))
				raise ValueError("UnsupportedType")
			logging.debug(content)
			logging.debug(url)
			# Start the async transaction to get the plain text
			worker_req_thread = BoilerPipeWorker(content)
			worker_req_thread.start()

			# Whilst that's executing, parse the document 
			logging.info("Parsing HTML...")
			html = BeautifulSoup(content)

			# Extract the dates 
			date_dict = pydate.get_dates(html)
			logging.debug(date_dict)
			if len(date_dict) == 0:
				status = "NoDates"

			# Wait for the BoilerPipe thread to complete
			worker_req_thread.join()
			logging.debug(worker_req_thread.result)
			logging.debug(worker_req_thread.version)

			if worker_req_thread.result == None:
				raise ValueError("NoContent")

			# Run keyword extraction 

			# Run sentiment analysis

		except ValueError as ex:
			status = ex.message

		# Build database objects 
		domain = self.dc.get_Domain_fromurl(url)
		self.dc.commit()

		path   = self.ac.get_path_fromurl(url)

		logging.debug("Domain: %s", domain)
		logging.debug("Path: %s", path)

		article = Article(path, date_crawled, crawl_id, domain, status)
		self.ac.attach_Article(article)

		# Commit to database, return True on success
		self.dc.commit()
		self.ac.commit()
		return True 


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
			parsed  = etree.fromstring(r.text)
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