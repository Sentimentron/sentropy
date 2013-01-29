#!/usr/bin/env python

from sqlalchemy import Table, Sequence, Column, String, Integer, UniqueConstraint, ForeignKey, create_engine
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm import validates, relationship
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy.types import Enum, DateTime
from sqlalchemy.orm.exc import *
from datetime import datetime

from controller import DBBackedController

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
		return [i.id for i in it]

	def get_CrawlFile_fromid(self, identifier):

		it = self._session.query(CrawlFile).filter_by(id=identifier)

		try:
			it = it.one()
		except NoResultFound:
			return None 

		return it

	def deduplicate(self):
		logging.debug("Deduplicating...")
		sql = "DELETE FROM crawl_files WHERE id NOT IN (SELECT MIN(id) FROM crawl_files GROUP BY key, source_id)"
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