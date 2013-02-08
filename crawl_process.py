#!/usr/bin/env python

# Crawl process helper

import multiprocessing
import logging
import os
import sys

import core
import requests

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *

from backend import CrawlQueue, CrawlFileController, CrawlProcessor, ProcessQueue
from backend.db import SoftwareVersionsController, SoftwareVersion, RawArticle


def main():
    core.configure_logging()

    multi   = "--multi" in sys.argv

    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
    logging.info("Binding session...")
    session = Session(bind=engine, autocommit = False)

    p  = ProcessQueue()
    cp = CrawlProcessor(engine)
    sw = SoftwareVersionsController(engine)

    for article_id in p: 
        article = session.query(RawArticle).get(article_id)
        if article is None:
            logging.error("%d doesn't exist!")
            continue 

        if article.status != "Unprocessed":
            logging.error("%d@%s: status %s", article_id, article.url, article.status)

        status = cp.process_record((article.crawl_id, (article.headers, article.content, article.url, \
            article.date_crawled, article.content_type)))
        if status is None:
            article.status = "Error"
        else:
            article.status = "Processed"
            article.inserted_id = status

        logging.info("%d: status changed to %s", article.id, article.status)
        session.commit()
        p.set_completed(article_id)

if __name__ == "__main__":
    main()