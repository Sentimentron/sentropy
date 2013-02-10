#!/usr/bin/env python

import sys
import logging
import core

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *

from backend.db import UserQuery, UserQueryKeywordRecord, UserQueryDomainRecord, UserQueryArticleRecord
from backend.db import Keyword, Domain

if __name__ == "__main__":

    #
    # Argument processing
    query_args = sys.argv[1:]
    query_text = ' '.join(query_args)

    #
    # Database connection
    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
    logging.info("Binding session...")
    session = Session(bind=engine, autocommit = False)

    #
    # Initial query object

    it = session.query(UserQuery).filter_by(text = query_text)
    try:
        q = it.one()
    except NoResultFound:
        q = UserQuery(query_text)

    session.add(q)
    session.commit()
    assert q.id is not None 

    #
    # Query keyword resolution
    keywords = set([])
    for k in q.get_keywords(q.text):
        it = session.query(Keyword).filter(Keyword.word.like("%s" % (k,)))
        keywords.update(it)

    #
    # Query domain resolution
    domains = set([])
    for k in q.get_domains(q.text):
        it = session.query(Domain).filter(Domain.key.like("%%%s" % (k,)))
        domains.update(it)

    print domains
