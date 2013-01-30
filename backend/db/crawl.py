#!/usr/bin/env python

import logging
import re

from sqlalchemy import Table, Sequence, Column, String, Integer, UniqueConstraint, ForeignKey, create_engine
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm import validates, relationship
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy.types import Enum, DateTime
from sqlalchemy.orm.exc import *
from datetime import datetime

from controller import DBBackedController

KEY_VAL = re.compile("^([a-z0-9]([-a-z0-9]*[a-z0-9])?\\.)+((a[cdefgilmnoqrstuwxz]|aero|arpa)|(b[abdefghijmnorstvwyz]|biz)|(c[acdfghiklmnorsuvxyz]|cat|com|coop)|d[ejkmoz]|(e[ceghrstu]|edu)|f[ijkmor]|(g[abdefghilmnpqrstuwy]|gov)|h[kmnrtu]|(i[delmnoqrst]|info|int)|(j[emop]|jobs)|k[eghimnprwyz]|l[abcikrstuvy]|(m[acdghklmnopqrstuvwxyz]|mil|mobi|museum)|(n[acefgilopruz]|name|net)|(om|org)|(p[aefghklmnrstwy]|pro)|qa|r[eouw]|s[abcdeghijklmnortvyz]|(t[cdfghjklmnoprtvwz]|travel)|u[agkmsyz]|v[aceginu]|w[fs]|y[etu]|z[amw])$")

Base = declarative_base()

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

	def __init__(self, engine):
		super(CrawlController, self).__init__(engine, Base)

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
class Domain(Base):

	__tablename__ = 'domains'

	id 		= Column(Integer, Sequence('domain_id_seq'), primary_key = True)
	key 	= Column(String(255), nullable = False, unique = True)
	date    = Column(DateTime, nullable = False)

	@classmethod
	def is_valid(cls, value):
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

	def __init__(self, engine):
		super(DomainController, self).__init__(engine, Base)

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

class Article(Base):

	__tablename__ = 'articles'

	id 		= Column(Integer, Sequence('article_id_seq'), primary_key = True)
	path 	= Column(String(2083), nullable=False)
	crawled = Column(DateTime, nullable = False)
	inserted= Column(DateTime, nullable = False)
	crawl_id= Column(Integer, ForeignKey("crawl_files.id"), nullable = True)
	domain_id = Column(Integer, ForeignKey("domains.id"), nullable = False)
	status  = Column(Enum("Processed", "NoDates", "NoContent", "UnsupportedType", "OtherError"), nullable = False)

	domain = relationship("Domain")

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

	def __init__(self, path, crawled, crawl_id, domain, status):
		self.path = path 
		self.crawled = crawled
		self.crawl_id = crawl_id
		self.status = status
		self.inserted = datetime.now()
		self.domain_id = domain.id

class ArticleController(DBBackedController):

	def __init__(self, engine):
		super(ArticleController, self).__init__(engine, Base)

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

