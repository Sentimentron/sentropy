#!/usr/bin/env python

import logging
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *

from backend import CrawlQueue, CrawlFileController, CrawlProcessor, ProcessQueue
from backend.db import RawArticle, CrawlController
import core

def main():

    core.configure_logging()

    p = ProcessQueue()

    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
    logging.info("Binding session...")
    session = Session(bind=engine, autocommit = False)

    for article in session.query(RawArticle).filter_by(status = 'Unprocessed'):
    	p.add_id(article.id)

if __name__ == '__main__':
    main()