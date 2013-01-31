#!/usr/bin/env

import types
import logging

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session 

class DBBackedController(object):

	def __init__(self, engine, base_ref):

		if type(engine) == types.StringType:
			logging.info("Using connection string '%s'" % (engine,))
			new_engine = create_engine(engine, encoding='utf-8')
			if "sqlite:" in engine:
				logging.debug("Setting text factory for unicode compat.")
				new_engine.raw_connection().connection.text_factory = str 
			self._engine = new_engine
			logging.info("Binding session...")
			self._session = Session(bind=self._engine)
		elif type(engine) is Session:
			logging.info("Using existing Session...")
			self._session = engine
		else:
			logging.info("Using existing engine...")
			self._engine = engine
			logging.info("Binding session...")
			self._session = Session(bind=self._engine)
		logging.info("Updating metadata...")
		base_ref.metadata.create_all(self._engine)

	def commit(self):
		logging.info("Commiting...")
		self._session.commit()