#!/usr/bin/env python

# Crawl process helper

import multiprocessing
import logging
import os
import sys

import core
import requests
import multiprocessing

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *

from backend import CrawlQueue, CrawlFileController, CrawlProcessor, ProcessQueue
from backend.db import SoftwareVersionsController, SoftwareVersion, RawArticle

def worker_init():
    global cp 
    global engine 
    global session

    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
    cp = CrawlProcessor(engine)
    session = Session(bind=engine, autocommit = False)


def worker_func(article_id):
    article = session.query(RawArticle).get(article_id)
    if article is None:
        logging.error("%d doesn't exist!")
        return None 

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
    return article_id

def main():
    core.configure_logging("crawl_process")

    multi   = "--multi" in sys.argv

    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
    logging.info("Binding session...")
    session = Session(bind=engine, autocommit = False)

    p  = ProcessQueue()
    pool = multiprocessing.Pool(None, worker_init)

    ids  = pool.imap(worker_func, p)
    for article_id in ids:
        if article_id is None:
            continue
        p.set_completed(article_id)

if __name__ == "__main__":
    main()