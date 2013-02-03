#!/usr/bin/env python

import logging
import re
import types

from sqlalchemy import Table, Sequence, Float, Column, String, Integer, UniqueConstraint, ForeignKey, create_engine
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm import validates, relationship
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy.types import Enum, DateTime, SmallInteger
from sqlalchemy.orm.exc import *
from datetime import datetime

KEY_VAL = re.compile("^([a-z0-9]([-a-z0-9]*[a-z0-9])?\\.)+((a[cdefgilmnoqrstuwxz]|aero|arpa)|(b[abdefghijmnorstvwyz]|biz)|(c[acdfghiklmnorsuvxyz]|cat|com|coop)|d[ejkmoz]|(e[ceghrstu]|edu)|f[ijkmor]|(g[abdefghilmnpqrstuwy]|gov)|h[kmnrtu]|(i[delmnoqrst]|info|int)|(j[emop]|jobs)|k[eghimnprwyz]|l[abcikrstuvy]|(m[acdghklmnopqrstuvwxyz]|mil|mobi|museum)|(n[acefgilopruz]|name|net)|(om|org)|(p[aefghklmnrstwy]|pro)|qa|r[eouw]|s[abcdeghijklmnortvyz]|(t[cdfghjklmnoprtvwz]|travel)|u[agkmsyz]|v[aceginu]|w[fs]|y[etu]|z[amw])$")

Base = declarative_base()

class DBBackedController(object):

	def __init__(self, engine, session=None):

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

		if session is None:
			logging.info("Binding session...")
			self._session = Session(bind=self._engine)
		else:
			self._session = session

		logging.info("Updating metadata...")
		Base.metadata.create_all(self._engine)

	def commit(self):
		logging.info("Commiting...")
		self._session.commit()

class CrawlSource(Base):

	__tablename__ = 'crawl_sources'

	id 		= Column(Integer, Sequence('crawl_source_id_seq'), primary_key = True)
	key	    = Column(String(1024), nullable = False, unique = True)

	@validates('key')
	def validate_key(self, key, val):
		assert len(val) > 0
		return val

	def __init__(self, key):
		self.key = key

class CrawlFile(Base):

	__tablename__ = 'crawl_files'

	id 		= Column(Integer, Sequence('crawl_id_seq'), primary_key = True)
	key 	= Column(String(1024), nullable = False)
	status 	= Column(Enum("Complete", "Incomplete", "Error"), nullable = False)
	kind    = Column(Enum("SQL", "Text", "ARFF"), nullable = False)
	source_id = Column(Integer, ForeignKey("crawl_sources.id"), nullable = False)

	date_loaded = Column(DateTime, nullable = False)
	date_update = Column(DateTime, nullable = False)

	src = relationship("CrawlSource")

	@validates('key')
	def validate_key(self, key, val):
		assert len(val) > 0
		return val

	def __init__(self, key, src,  kind, status="Incomplete", date_loaded = datetime.now(), date_updated = datetime.now()):

		if not isinstance(src, CrawlSource):
			raise TypeError(type(src))

		self.key = key 
		self.src = src
		self.type = type 
		self.kind = kind
		self.status = status
		self.date_loaded = date_loaded
		self.date_update = date_updated

class CrawlController(DBBackedController):

	def __init__(self, engine, session = None):
		super(CrawlController, self).__init__(engine, session)

	def get_CrawlSource(self, key, limit=1):
		ret = self._session.query(CrawlSource).filter_by(key=key).limit(limit)
		try:
			ret = ret.one()
		except NoResultFound:
			return None 

		return ret

	def get_CrawlFiles(self, keys, src=None):

		it = self._session.query(CrawlFile).filter(CrawlFile.key.in_(keys))
		if src is not None:
			it = it.filter_by(src=src)

		return it.all()

	def get_CrawlFile(self, key, limit=1, src=None):

		it = self._session.query(CrawlFile).filter_by(key=key)
		if src is not None:
			it = it.filter_by(src=src)

		it = it.limit(limit)
		try:
			it = it.one()
		except NoResultFound:
			return None 

		return it

	def get_randomCrawlIdentifiers(self, limit=100):
		from sqlalchemy.sql.expression import func
		it = self._session.query(CrawlFile).filter_by(status = "Incomplete").order_by(func.rand()).limit(limit)
		return [i.id for i in it if "crawl-002" not in i.key]

	def get_CrawlFile_fromid(self, identifier):

		it = self._session.query(CrawlFile).filter_by(id=identifier)

		try:
			it = it.one()
		except NoResultFound:
			return None 

		return it

	def deduplicate(self):
		logging.debug("Deduplicating...")
		sql = "DELETE FROM crawl_files WHERE id NOT IN (SELECT id FROM (SELECT MIN(id) as id FROM crawl_files GROUP BY `key`, source_id) AS tmp);" 
		self._engine.execute(sql)
		logging.debug("Deduplication finished.")

	def attach_CrawlSource(self, src):
		if not isinstance(src, CrawlSource):
			raise TypeError((src, type(src)))

		self._session.add(src)

	def attach_CrawlFile(self, src):
		if not isinstance(src, CrawlFile):
			raise TypeError((src, type(src)))

		self._session.add(src)

class KeywordIncidence(Base):

	__tablename__ = 'keyword_incidences'
	id 		   = Column(Integer, Sequence('keywordincidence_id_seq'), primary_key = True)
	keyword_id = Column(Integer, ForeignKey('keywords.id'), nullable = False)
	phrase_id  = Column(Integer, ForeignKey('phrases.id'),  nullable = False)

	def __init__(self, keyword, phrase):

		if not isinstance(keyword, Keyword):
			raise TypeError(("Must be a Keyword", keyword, type(keyword)))

		if not isinstance(phrase, Phrase):
			raise TypeError(("Must be a Phase", phrase, type(phrase)))

		self.keyword = keyword
		self.phrase  = phrase 

class Keyword(Base):

	MAX_LENGTH = 32

	__tablename__ = 'keywords'
	id 		= Column(Integer, Sequence('keyword_id_seq'), primary_key = True)
	word    = Column(String(MAX_LENGTH), nullable = False, unique = True)
	incidences = relationship("KeywordIncidence", backref="keyword")

	@validates('word')
	def validate_keyword(self, key, word):
		word = word.strip()

		if len(word) == 0:
			raise ValueError(("Too short", word))

		if len(word) > self.MAX_LENGTH:
			raise ValueError(("Too long", word))

		valid = True 
		for pos, char in enumerate(word):
			valid = char >= 'a' and char <='z'
			valid = valid or (char >= 'A' and char <='Z')
			valid = valid or (char >= '0' and char <='9')
			valid = valid or (char == ' ')
			if not valid:
				raise ValueError("Invalid character '%s' in '%s' at position %d" % (char, word, pos))

		return word 

	def __init__(self, keyword):
		self.word = keyword 

	def __len__(self):
		return len(self.word)

	def __str__(self):
		return "Keyword(%s)" % (self.word)

class Phrase(Base):

	__tablename__ = 'phrases'
	id       = Column(Integer, Sequence('phrase_id_seq'), primary_key = True)
	sentence = Column(Integer, ForeignKey("sentences.id"), nullable = False)
	score    = Column(Float, nullable = False)
	prob     = Column(Float, nullable = False)
	label    = Column(Enum("Positive", "Unknown", "Negative"), nullable = False)

	keyword_incidences = relationship("KeywordIncidence", backref="phrase")

	@validates('prob')
	def validate_prob(self, key, val):
		assert val >= 0 and val <= 1
		return val 

	@validates('score')
	def validate_score(self, key, score):
		assert score >= -1 and score <= 1
		return score 

	def __init__(self, parent, score, prob, label):

		if not isinstance(parent, Sentence):
			raise TypeError(("parent: should be Sentence", parent, type(parent)))

		self.parent = parent

		# Set the label
		if label == 1:
			self.label = "Positive"
		elif label == 0:
			self.label = "Unknown"
		elif label == -1:
			self.label = "Negative"
		else:
			raise ValueError(("Invalid label", label))

		self.score = score 
		self.prob  = prob

class KeywordController(DBBackedController):

	def __init__(self, engine, session = None):
		super(KeywordController, self).__init__(engine, session)

	def get_Keyword(self, term):
		it = self._session.query(Keyword).filter_by(word = term)
		try:
			return it.one()
		except NoResultFound:
			logging.debug("NoResultFound for %s" % (term,))
			ret = Keyword(term)
			self._session.add(ret)
			return ret

class Sentence(Base):

	__tablename__ = 'sentences'

	id       = Column(Integer, Sequence('sentence_id_seq'), primary_key = True)
	document = Column(Integer, ForeignKey('documents.id'), nullable = False )
	score    = Column(Float, nullable = False)
	prob     = Column(Float, nullable = False)
	label    = Column(Enum("Positive", "Unknown", "Negative"), nullable = False)
	level    = Column(Enum("H1", "H2", "H3", "H4", "H5", "H6", "P", "Other", "Unknown"), nullable = False)

	phrases = relationship("Phrase", backref="parent")

	@validates('prob')
	def validate_prob(self, key, val):
		assert val >= 0 and val <= 1
		return val 

	@validates('score')
	def validate_score(self, key, score):
		assert score >= -1 and score <= 1
		return score 

	def __init__(self, parent, label, score, prob, level):

		if not isinstance(parent, Document):
			raise TypeError(("parent: should be Sentence", parent, type(parent)))

		self.parent = parent

		# Set the label
		if label == 1:
			self.label = "Positive"
		elif label == 0:
			self.label = "Unknown"
		elif label == -1:
			self.label = "Negative"
		else:
			raise ValueError(("Invalid label", label))

		self.score = score 
		self.prob  = prob
		self.level = level

class SoftwareInvolvementRecord(Base):

	__tablename__ = "software_involvements"

	id          = Column(Integer, Sequence('sinvolved_id_seq'), primary_key = True)
	document_id = Column(Integer, ForeignKey("documents.id"), nullable = False)
	software_id = Column(Integer, ForeignKey("software.id"), nullable = False)
	action      = Column(Enum("Classified", "Dated", "Processed", "Extracted", "Other"), nullable = False)

	def __init__(self, software, action, document):

		if not isinstance(document, Document):
			raise TypeError(("Not a Document", document, type(document)))

		if not isinstance(software, SoftwareVersion):
			raise TypeError(("Not a SoftwareVersion", software, type(software)))

		self.action = action 
		self.software = software 
		self.document = document 

class CertainDate(Base):

	__tablename__ = 'certain_dates'

	id 		= Column(Integer, Sequence('certain_date_id_seq'), primary_key = True)
	date 	= Column(DateTime, nullable = False)
	doc_id 	= Column(Integer, ForeignKey("documents.id"), nullable = False)

	def __init__(self, date, document):

		if not isinstance(document, Document):
			raise TypeError(("document: Not a Document", document, type(document)))

		self.document = document 
		self.date = date 

class AmbiguousDate(Base):

	__tablename__ = 'uncertain_dates'

	MAX_FRAG_LEN = 32

	id 		= Column(Integer, Sequence('certain_date_id_seq'), primary_key = True)
	date 	= Column(DateTime, nullable = False)
	doc_id 	= Column(Integer, ForeignKey("documents.id"), nullable = False)
	interpreted_with 	= Column(Enum("DayFirstYearFirst", "DayFirstYearSecond", "DaySecondYearFirst", "DaySecondYearSecond"), nullable = False)
	matched_text 		= Column(String(MAX_FRAG_LEN), nullable = False)

	@validates('matched_text')
	def validate_text(self, key, value):
		value = value.strip()
		if len(value) > 0:
			if len(value) <= self.MAX_FRAG_LEN:
				return value 
			raise ValueError(("Too long", value))
		raise ValueError(("Too short", value))

	def __init__(self, date, document, day_first, year_first, text):

		if not isinstance(document, Document):
			raise TypeError(("document: Not a Document", document, type(document)))

		if day_first:
			if year_first:
				self.interpreted_with = "DayFirstYearFirst"
			else:
				self.interpreted_with = "DayFirstYearSecond"
		else:
			if year_first:
				self.interpreted_with = "DaySecondYearFirst"
			else:
				self.interpreted_with = "DaySecondYearSecond"

		self.date = date 
		self.document = document
		self.matched_text = text

class KeywordAdjacency(Base):

	__tablename__ = "keyword_adjacencies"

	id 		= Column(Integer, Sequence('keyword_adj_seq'), primary_key = True)
	doc_id  = Column(Integer, ForeignKey("documents.id"), nullable = False)
	key1_id = Column(Integer, ForeignKey("keywords.id"), nullable = False)
	key2_id = Column(Integer, ForeignKey("keywords.id"), nullable = True)

	key1 = relationship("Keyword", foreign_keys=[key1_id])
	key2 = relationship("Keyword", foreign_keys=[key2_id])

	def __init__(self, key1, key2, document):
		if not isinstance(document, Document):
			raise TypeError(("document: Not a Document", document, type(document)))

		for pos, key in enumerate([key1, key2]):
			if not isinstance(key, Keyword):
				raise TypeError(("key%d: Must be a Keyword" % (pos+1,), key, type(key)))

		self.document = document 
		self.key1 = key1 
		self.key2 = key2


class RelativeLink(Base):

	__tablename__ = 'links_relative'

	id          = Column(Integer, Sequence('internal_link_id_seq'), primary_key = True)
	path        = Column(String(1024), nullable = False)
	document_id = Column(Integer, ForeignKey("documents.id"), nullable = False)

	@validates('path')
	def validate(self, key, path):

		if len(path) == 0:
			raise ValueError("Path is too short")
		if len(path) > 1024:
			raise ValueError(("Path is too long", path))

		if "http://" in path or "://" in path:
			raise ValueError(("RelativeLinks should not contain a prefix", path))
		return path

	def __str__(self):
		return "RelativeLink(%s)" % (self.path,)

	def __init__(self, document, path): 
		if not isinstance(document, Document):
			raise TypeError(("document: Not a Document", document, type(document)))

		self.document = document 
		self.path     = path 

class AbsoluteLink(Base):

	__tablename__ = 'links_absolute'

	id          = Column(Integer, Sequence('internal_link_id_seq'), primary_key = True)
	path        = Column(String(1024), nullable = False)
	domain_id   = Column(Integer, ForeignKey("domains.id"), nullable = False)
	document_id = Column(Integer, ForeignKey("documents.id"), nullable = False)

	@validates('path')
	def validate(self, key, path):
		path = path.strip()
		if len(path) == 0:
			raise ValueError("Path is too short")
		if len(path) > 1024:
			raise ValueError(("Path is too long", path))

		if "http://" in path or "://" in path:
			raise ValueError(("AbsoluteLinks should not contain a prefix", path))
		return path

	def __str__(self):
		return "AbsoluteLink (%s/%s)" % (self.domain.key, self.path)

	def __init__(self, document, domain, path): 
		if not isinstance(document, Document):
			raise TypeError(("document: Not a Document", document, type(document)))

		if not isinstance(domain, Domain):
			raise TypeError(("domain: Not a Domain", domain, type(domain)))

		self.document = document 
		self.path     = path 
		self.domain   = domain

class Document(Base):

	__tablename__ = "documents"

	id          = Column(Integer, Sequence('document_id_seq'), primary_key = True)
	article_id  = Column(Integer, ForeignKey('articles.id'), nullable = False)
	length      = Column(SmallInteger, nullable = False)
	label       = Column(Enum("Positive", "Unknown", "Negative"), nullable = False)
	headline    = Column(String(256), nullable = False)

	pos_phrases = Column(SmallInteger, nullable = False)
	neg_phrases = Column(SmallInteger, nullable = False)
	pos_sentences = Column(SmallInteger, nullable = False)
	neg_sentences = Column(SmallInteger, nullable = False)

	sentences = relationship("Sentence", backref="parent")
	involved  = relationship("SoftwareInvolvementRecord", backref="document")
	certain_dates = relationship("CertainDate", backref="document")
	uncertain_dates = relationship("AmbiguousDate", backref="document")
	keyword_adjacencies = relationship("KeywordAdjacency", backref="document")

	relative_links = relationship("RelativeLink", backref="document")
	absolute_links = relationship("AbsoluteLink", backref="document")


	@validates('prob')
	def validate_prob(self, key, val):
		if val < 0 or val > 1:
			raise ValueError(("Invalid probability", val))
		return val 

	@validates('pos_phrases', 'neg_phrases', 'pos_sentences', 'neg_sentences')
	def validate_scores(self, key, score):
		if score < 0:
			raise ValueError(("Scores shouldn't be negative", key, score))
		return score

	@validates('length')
	def validate_length(self, key, length):
		if length == 0:
			raise ValueError("Needs more length.")
		return length

	@validates('headline')
	def validate_headline(self, key, headline):
		if len(headline) == 0:
			raise ValueError("Headline is too short")
		if len(headline) > 256:
			raise ValueError("Headline is too long")
		return headline 

	def __init__(self, parent, label, length, pos_sentences, neg_sentences, pos_phrases, neg_phrases, headline):

		if not isinstance(parent, Article):
			raise TypeError(("parent: should be Article", parent, type(parent)))

		self.parent = parent

		self.length = length 
		self.pos_phrases   = pos_phrases
		self.neg_phrases   = neg_phrases
		self.pos_sentences = pos_sentences
		self.neg_sentences = neg_sentences
		self.headline = headline

		# Set the label
		if label == 1:
			self.label = "Positive"
		elif label == 0:
			self.label = "Unknown"
		elif label == -1:
			self.label = "Negative"
		else:
			raise ValueError(("Invalid label", label))

class Article(Base):

	__tablename__ = 'articles'

	id 		= Column(Integer, Sequence('article_id_seq'), primary_key = True)
	path 	= Column(String(2083), nullable=False)
	crawled = Column(DateTime, nullable = False)
	inserted= Column(DateTime, nullable = False)
	crawl_id= Column(Integer, ForeignKey("crawl_files.id"), nullable = True)
	domain_id = Column(Integer, ForeignKey("domains.id"), nullable = False)
	status  = Column(Enum("Processed", "NoDates", "NoContent", "UnsupportedType", "ClassificationError", "OtherError"), nullable = False)

	documents = relationship("Document", backref="parent")

	@validates('path')
	def validate_path(self, key, value):
		if "http://" in value:
			raise ValueError("path: shouldn't be a URL: %s", (value,))

		for pos, char in enumerate(value):
			if char == '/':
				break

		sub_path = value[:pos]
		try:
			Domain.is_valid(sub_path)
		except ValueError:
			return value 

	def __str__(self):
		return "Article(%s)" % ([self.id, self.path, self.crawled, self.inserted, self.crawl_id, self.domain_id, self.status])

	def __init__(self, path, crawled, crawl_id, domain, status):

		if not isinstance(domain, Domain):
			raise TypeError(("Must be a Domain", domain, type(domain)))

		self.path = path 
		self.crawled = crawled
		self.crawl_id = crawl_id
		self.status = status
		self.inserted = datetime.now()
		self.domain = domain

class ArticleController(DBBackedController):

	def __init__(self, engine, session = None):
		super(ArticleController, self).__init__(engine, session)

	@classmethod
	def get_path_fromurl(cls, url):
		if "http://" in url:
			url = url[7:]

		for pos, char in enumerate(url):
			if char == '/':
				break

		key = url[pos:]
		return key 

	def attach_Article(self, article):
		if type(article) is not Article:
			raise TypeError(("Must be an Article", article, type(article)))

		self._session.add(article)

class SoftwareVersion(Base):

	__tablename__ = 'software'

	id       = Column(Integer, Sequence('software_version_id_seq'), primary_key = True)
	software = Column(String(256), unique = True)

	involved_with = relationship("SoftwareInvolvementRecord", backref="software")

	@validates('software')
	def validate_software_version(self, key, val):
		val = val.strip()
		assert len(val) > 0
		return val

	def __init__(self, version):
		self.software = version 

class SoftwareVersionsController(DBBackedController):

	def __init__(self, engine, session = None):
		super(SoftwareVersionsController, self).__init__(engine, session)

	def get_SoftwareVersion_fromstr(self, version):
		it = self._session.query(SoftwareVersion).filter_by(software = version)
		try:
			return it.one()
		except NoResultFound:
			return SoftwareVersion(version)

class Domain(Base):

	__tablename__ = 'domains'

	id 		= Column(Integer, Sequence('domain_id_seq'), primary_key = True)
	key 	= Column(String(255), nullable = False, unique = True)
	date    = Column(DateTime, nullable = False)

	articles = relationship("Article", backref="domain")
	absolute_links = relationship("AbsoluteLink", backref="domain")

	@classmethod
	def is_valid(cls, value):
		value = value.strip()
		if len(value) == 0:
			raise ValueError(("Domain is too short", value))
		if re.match(KEY_VAL, value) is None:
			raise ValueError(("Not a valid domain", value))
		if value[0] == '.':
			raise ValueError(("Not a valid domain", value))
		if len(value) > 255:
			raise ValueError(("Domain is too long", value))


	@validates('key')
	def validate_domain_key(self, key, value):
		self.is_valid(value)

		return value 

	@validates('date')
	def validate_date(self, key, value):
		if type(value) is not datetime:
			raise TypeError(("Not a datetime", value, type(value)))

		return value

	def __str__(self):
		return "Domain(%s)" % (self.key,)

	def __repr__(self):
		return "Domain(%s|%s)" % (self.key, self.date)

	def __init__(self, key):
		self.key = key 
		self.date = datetime.now()

class DomainController(DBBackedController):

	def __init__(self, engine, session = None):
		super(DomainController, self).__init__(engine, session)

	def get_Domain(self, key):
		it = self._session.query(Domain).filter_by(key=key)
		try:
			it = it.one()
		except NoResultFound:
			return None 

		return it

	def get_Domain_fromurl(self, url):
		orig = url
		if "http://" in url:
			url = url[7:]

		for pos, char in enumerate(url):
			if char == '/':
				break

		key = url[:pos]

		# Search the database
		d = self.get_Domain(key)
		if d is None:
			d = Domain(key)
			self.attach_Domain(d)
			return d

		return d


	def attach_Domain(self, domain):
		if type(domain) != Domain:
			raise TypeError("Not a domain: %s" % (domain,))

		self._session.add(domain)