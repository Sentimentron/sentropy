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

    c = CrawlController(core.get_database_engine_string())
    q = CrawlQueue(c)
    r = CrawlFileController(c)
    p = ProcessQueue()

    core.configure_logging()
    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
    logging.info("Binding session...")
    session = Session(bind=engine, autocommit = False)
            
    for crawl_file in q:
        records = r.read_CrawlFile(crawl_file)
        for record in records:
            headers, content, url, date_crawled, content_type = record 
            headers, content, url = [str(i) for i in [headers, content, url]]
            crawl_id = crawl_file.id 
            record = (crawl_id, (headers, content, url, date_crawled, content_type))

            try:
                it = session.query(RawArticle).filter_by(url=url).filter_by(crawl_id=crawl_id).filter_by(date_crawled=date_crawled)
                it = it.one()
                logging.info("Article already exists!")
                continue 
            except NoResultFound:
                logging.debug("Article doesn't exist!")

            a = RawArticle(record)
            logging.info("Article: %s", a.url)
            session.add(a)
            session.commit()
            assert a.id is not None 
            p.add_id(a.id)

        r.mark_CrawlFile_complete(crawl_file)
        q.set_completed(crawl_file)



if __name__ == '__main__':
    main()