#!/usr/bin/env python

import logging
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm import *

from backend import CrawlQueue, CrawlFileController, CrawlProcessor, ProcessQueue
from backend.db import CrawlFile, RawArticle, CrawlController
import core

def main():

    core.configure_logging()

    c = CrawlController(core.get_database_engine_string())
    r = CrawlFileController(c)
    p = ProcessQueue()

    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
    logging.info("Binding session...")
    session = Session(bind=engine, autocommit = False)

    it = session.query(CrawlFile).filter_by(status = 'Incomplete').limit(1)

    if "--files" in sys.argv:            
        for crawl_file in it:
            records = r.read_CrawlFile(crawl_file)
            if records is None:
                continue
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
    if "--documents" in sys.argv:
	it = session.execute("SELECT id FROM raw_articles WHERE id NOT IN (SELECT raw_article_id FROM raw_article_results)")
        for i, in it:
            logging.info(i)
            p.add_id(i)
    
    logging.info("Crawl process completed.")

if __name__ == '__main__':
    main()
