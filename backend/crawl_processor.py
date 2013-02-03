#!/usr/bin/env python

# Crawl Processor 
import itertools
import logging
import os
import threading
import types

from collections import Counter

from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session 
from topia.termextract import extract
from nltk.tokenize import sent_tokenize
import nltk
from lxml import etree
from bs4 import BeautifulSoup
from pysen.documents import DocumentClassifier

import pydate
import pysen
import pysen.models

from db import Article, Domain, DomainController, ArticleController
from db import Keyword, KeywordController
from db import SoftwareVersionsController
from db import Document, Sentence, Phrase
from db import KeywordIncidence, SoftwareInvolvementRecord

KEYWORD_LIMIT = 1024

class KeywordSet(object):

	def __init__(self, stop_list):
		self.keywords = set([])
		self.stop_list = stop_list

	def __len__(self):
		return len(self.keywords)

	def __str__(self):
		return "KeywordSet(%s)" % (str(self.keywords),)

	def __iter__(self):
		for item in self.keywords:
			yield item

	def add(self, term):
		
		if len(self) > KEYWORD_LIMIT:
			raise ValueError("KEYWORD_LIMIT exceeded.")

		term = term.lower().strip()

		if not term in self.stop_list:
			self.keywords.add(term)
			return True 

		return False 

	def convert(self, kwc):
		ret = []
		short = []
		for t in self.keywords:
			try:
				ret.append(kwc.get_Keyword(t))
			except ValueError as ex:
				logging.error(ex)

		return ret, short

class CrawlProcessor(object):

	__VERSION__ = "CrawlProcessor-0.1"

	def __init__(self, engine, stop_list="keyword_filter.txt"):

		if type(engine) == types.StringType:
			logging.info("Using connection string '%s'" % (engine,))
			new_engine = create_engine(engine, encoding='utf-8')
			if "sqlite:" in engine:
				logging.debug("Setting text factory for unicode compat.")
				new_engine.raw_connection().connection.text_factory = str 
			self._engine = new_engine
		else:
			logging.info("Using existing engine...")
			self._engine = engine
		logging.info("Binding session...")
		self._session = Session(bind=self._engine, autocommit=False)

		if type(stop_list) == types.StringType:
			stop_list_fp = open(stop_list)
		else:
			stop_list_fp = stop_list 

		self.stop_list = set([])
		for line in stop_list_fp:
			self.stop_list.add(line.strip())

		self.cls = DocumentClassifier()
		self.dc  = DomainController(self._engine, self._session)
		self.ac  = ArticleController(self._engine, self._session)
		self.ex  = extract.TermExtractor()
		self.kwc = KeywordController(self._engine, self._session)
		self.swc = SoftwareVersionsController(self._engine, self._session)

	def process_record(self, item):

		crawl_id, record = item

		headers, content, url, date_crawled, content_type = [str(i) for i in record]
		assert headers is not None
		assert content is not None 
		assert url is not None 
		assert date_crawled is not None 
		assert content_type is not None 

		status = "Processed"
		# Build database objects 
		self._session.begin(subtransactions=True)
		domain = self.dc.get_Domain_fromurl(url)
		self._session.commit()
		path   = self.ac.get_path_fromurl(url)
		article = Article(path, date_crawled, crawl_id, domain, status)
		print article
		classified_by = self.swc.get_SoftwareVersion_fromstr(pysen.__VERSION__)
		assert classified_by is not None

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
		kset     = KeywordSet(self.stop_list)
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
		label, length, classified, pos_sentences, neg_sentences,\
		pos_phrases, neg_phrases  = features[0:7]

		# Convert Pysen's model into database models
		d = Document(article, label, length, pos_sentences, neg_sentences, pos_phrases, neg_phrases)
		self._session.add(d)
		extracted_phrases = set([])
		for sentence, score, phrase_trace in trace:
			sentence_type = "Unknown"
			for node in html.findAll(text=True):
				if sentence.text in node.strip():
					sentence_type = node.parent.name.upper()
					break

			if sentence_type not in ["H1", "H2", "H3", "H4", "H5", "H6", "P", "Unknown"]:
				sentence_type = "Other"

			label, average, prob, pos, neg, probs, _scores = score 

			s = Sentence(d, label, average, prob, sentence_type)
			self._session.add(s)
			for phrase, prob, score, label in phrase_trace:
				p = Phrase(s, score, prob, label)
				self._session.add(p)
				extracted_phrases.add((phrase, p))

		# Associate extracted keywords with phrases
		keyword_objects, short_keywords = kset.convert(self.kwc)
		for k in keyword_objects:
			self._session.add(k)
		for p, p_obj in extracted_phrases:
			for k in keyword_objects:
				if k.word in p.get_text():
					nk = KeywordIncidence(k, p_obj)

		# Construct software involvment records
		self_sir = SoftwareInvolvementRecord(self.swc.get_SoftwareVersion_fromstr(self.__VERSION__), "Processed", d)
		date_sir = SoftwareInvolvementRecord(self.swc.get_SoftwareVersion_fromstr(pydate.__VERSION__), "Dated", d)
		clas_sir = SoftwareInvolvementRecord(self.swc.get_SoftwareVersion_fromstr(pysen.__VERSION__), "Classified", d)
		self._session.add_all([self_sir, date_sir, clas_sir])

		logging.debug("Domain: %s", domain)
		logging.debug("Path: %s", path)
		article.status = status

		#self._session.add(domain)
		#self._session.add(article)
		#self._session.add(d)
		#self.ac.attach_Article(article)

		# Commit to database, return True on success
		self._session.commit()


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