import logging
import os
import sys

import core 

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm import *

from backend.db import RawArticle, RawArticleResultLink, RawArticleResult 

if __name__ == "__main__":

    core.configure_logging()

    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
    logging.info("Binding session...")
    session = Session(bind=engine, autocommit = False)

    # Select the old raw_results 
    sql = "SELECT crawl_id, date_crawled, url, content_type, raw_article_results.status, raw_article_conversions.inserted_id FROM raw_articles JOIN raw_article_results ON raw_article_results.raw_article_id = raw_articles.id JOIN raw_article_conversions ON raw_article_conversions.raw_article_id = raw_articles.id"
    it = session.execute(sql)

    for crawl_id, date_crawled, url, content_type, status, inserted_id in it:

        # Decide if any of these have been comitted
        sub = session.query(RawArticle).filter_by(crawl_id = crawl_id, url = url, content_type = content_type, date_crawled = date_crawled)
        try:
            i = sub.one()
            logging.info("RawArticle %s has already been processed.", i)
            continue 
        except NoResultException:
            pass


        rbase = RawArticle((crawl_id, (None, None, url, date_crawled, content_type)))
        rstat = RawArticleResult(None, status)
        rstat.parent = rbase 
        rrslt = RawArticleResultLink(None, inserted_id)
        rrslt.parent = rbase 
        session.add(rbase)
        session.add(rstat)
        session.add(rrslt)
        session.commit()