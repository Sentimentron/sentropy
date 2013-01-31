#!/usr/bin/env python

# Crawl Processor 
import itertools
import logging
import os
import threading
import types

from collections import Counter

import pysen.models

from topia.termextract import extract
from nltk.tokenize import sent_tokenize
import nltk
from lxml import etree
from bs4 import BeautifulSoup
from pysen.documents import DocumentClassifier

import pydate

from db import Article, Domain, DomainController, ArticleController
from db import Keyword, KeywordController

KEYWORD_LIMIT = 1024

class KeywordSet(object):

	def __init__(self):
		self.keywords = set([])

	def __len__(self):
		total = 0
		for i in self.keywords:
			total += len(i)
		return total 

	def __str__(self):
		return "KeywordSet(%s)" % (str(self.keywords),)

	def add(self, term):
		if len(term) + len(self) > KEYWORD_LIMIT:
			raise ValueError("KEYWORD_LIMIT exceeded.")
		self.keywords.add(term)

	def convert(self, kwc):
		return [kwc.get_Keyword(t) for t in self.keywords]

class CrawlProcessor(object):

	def __init__(self, engine):

		self.cls = DocumentClassifier()
		self.dc  = DomainController(engine)
		self.ac  = ArticleController(engine)
		self.ex  = extract.TermExtractor()
		self.kwc = KeywordController(engine)

	def process_record(self, item):

		crawl_id, record = item

		headers, content, url, date_crawled, content_type = [str(i) for i in record]
		status = "Processed"
		# Build database objects 
		domain = self.dc.get_Domain_fromurl(url)
		path   = self.ac.get_path_fromurl(url)
		article = Article(path, date_crawled, crawl_id, domain, status)

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

			content = worker_req_thread.result.encode('ascii', 'ignore')

			# Run keyword extraction 
			keywords = self.ex(content)
			kset     = KeywordSet()
			nnp_sets_scored = set([])

			for word, freq, amnt in sorted(keywords):
				try:
					nnp_sets_scored.add((word, freq))
				except ValueError:
					break 

			nnp_sets = set([])
			nnp_vector = []
			for sentence in sent_tokenize(content):
				text = nltk.word_tokenize(sentence)
				pos  = nltk.pos_tag(text)
				pos_groups = itertools.groupby(pos, lambda x: x[1])
				for k, g in pos_groups:
					if k != 'NNP':
						continue
					nnp_list = [word for word, pos in g]
					nnp_len  = min(3, len(nnp_list))
					cur = 1
					while cur <= nnp_len:
						for combo in itertools.combinations(nnp_list, cur):
							nnp_sets.add(combo)
							for n in combo:
								nnp_vector.append(n)
						cur += 1

			nnp_counter = Counter(nnp_vector)
			for item in nnp_sets:
				score = 0
				for word in item:
					score += nnp_counter[word]
				nnp_sets_scored.add((item, score*1.0 / len(item)))

			for item, score in sorted(nnp_sets_scored, key=lambda x: x[1], reverse=True):
				try: 
					if type(item) == types.ListType or type(item) == types.TupleType:
						kset.add(' '.join(item))
					else:
						kset.add(item)
				except ValueError:
					break 

			# Run sentiment analysis
			trace = []
			features = self.cls.classify(worker_req_thread.result, trace) 

			# Convert Pysen's model into database models
			for sentence, score, phrase_trace in trace:
				sentence_type = "Unknown"

				for node in html.findAll(text=True):
					if sentence.text in node.strip():
						sentence_type = node.parent.name
						break

				



		except ValueError as ex:
			status = ex.message

		logging.debug("Domain: %s", domain)
		logging.debug("Path: %s", path)

		article.status = status
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