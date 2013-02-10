#!/usr/bin/env python

# Crawl process helper

import multiprocessing
import logging
import os
import sys

import core
import itertools
import requests
import multiprocessing

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *

from backend import CrawlQueue, CrawlFileController, CrawlProcessor, ProcessQueue
from backend.db import SoftwareVersionsController, SoftwareVersion, RawArticle
from backend.db import RawArticleResult, RawArticleResultLink

def worker_init():
    global cp 
    global engine 
    global session

    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
    cp = CrawlProcessor(engine)
    session = Session(bind=engine, autocommit = False)


def has_article_been_processed(article_id):
    it = session.query(RawArticleResult).get(article_id)
    if it is None:
        return False 
    return True 

def worker_func(article_id):

    status = None 
    record = None 
    result_link = None 

    if has_article_been_processed(article_id):
        return article_id 

    article = session.query(RawArticle).get(article_id)
    if article is None:
        logging.error("Article doesn't exist: shouldn't be possible. %d", article_id)
        return None 

    status = cp.process_record((article.crawl_id, (article.headers, article.content, article.url, \
        article.date_crawled, article.content_type)))

    if status is None:
        record = RawArticleResult(article_id, "Error")
    else:
        record = RawArticleResult(article_id, "Processed")
        result_link = RawArticleResultLink(article_id, status)
        session.add(result_link)

    session.add(record)
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

    ids = None
    if multi:
        pool = multiprocessing.Pool(None, worker_init)
        ids  = pool.imap(worker_func, p, 2)
    else:
        worker_init()
        ids = itertools.imap(worker_func, p)

    for article_id in ids:
        if article_id is None:
            continue
        p.set_completed(article_id)

if __name__ == "__main__":
    main()