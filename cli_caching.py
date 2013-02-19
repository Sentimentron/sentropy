#!/usr/bin/env python

#
# Collection of redis support functions to assist caching / maintenance etc
#

import logging
import sys

import redis
from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *

import core
# Loads keywords out of the database and stores them in a redis instance
# (keyword => database_id) to assist insert

def get_redis_instance():
    host = core.get_redis_host()
    return redis.StrictRedis(host=host, port=6379, db=0)

def cache_keywords():
    core.configure_logging('debug')
    from backend.db import Keyword
    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ UNCOMMITTED")
    session = Session(bind=engine, autocommit = False)

    # Estimate the number of keywords
    logging.debug("Estimating number of keywords...")
    for count, in session.execute("SELECT COUNT(*) FROM keywords"):
        total = count 

    logging.debug("Establishing connection to redis...")
    r = get_redis_instance()

    logging.info("Caching %d keywords...", total)
    cached = 0
    for _id, word in session.execute("SELECT id, word FROM keywords"):
        r.set(k.word, k.id)
        cached += 1
        if cached % 1000 == 0:
            logging.info("Cached %d keywords (%.2f%% done)", it.count(), 100*cached)

    logging.info("Cached %d keywords (%.2f%% done)", it.count(), 100*cached)

if __name__ == "__main__":

    if "--keywords" in sys.argv:
        cache_keywords()