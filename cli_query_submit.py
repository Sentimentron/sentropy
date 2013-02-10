#!/usr/bin/env python

import math
import itertools
import sys
import logging
import core

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm import * 

from backend.db import UserQuery, UserQueryKeywordRecord, UserQueryDomainRecord, UserQueryArticleRecord
from backend.db import Keyword, Domain, KeywordAdjacency, Article, Document

def compute_likely_date(date_recs, certain = False):
    ret = None 
    if certain:
        mean = 346
    else:
        mean = 307 # Something, I don't know

    errors = [((p-mean)*(p-mean), j) for p, j in date_recs]
    least  = sorted(errors, key = lambda x: x[0])
    for error, rec in least:
        return rec 

if __name__ == "__main__":

    core.configure_logging()

    #
    # Argument processing
    query_args = sys.argv[1:]
    query_text = ' '.join(query_args)

    #
    # Database connection
    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8')
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
    logging.info("Resolving query id %d with text '%s'", q.id, q.text)
    #
    # Query keyword resolution
    keywords = set([])
    using_keywords = False 
    raw_keyword_count = 0
    for k in q.get_keywords(q.text):
        logging.debug("keyword: %s", k)
        using_keywords = True 
        raw_keyword_count += 1
        it = session.query(Keyword).filter(Keyword.word.like("%s" % (k,)))
        keywords.update(it)
    logging.info("Query(%d): expanded %d raw keywords into %d database terms", q.id, raw_keyword_count, len(keywords))

    #
    # Query domain resolution
    domains = set([])
    using_domains = False 
    raw_domain_count = 0
    for k in q.get_domains(q.text):
        logging.debug("domain: %s", k)
        raw_domain_count += 1
        using_domains = True 
        it = session.query(Domain).filter(Domain.key.like("%%%s" % (k,)))
        domains.update(it)
    logging.info("Query(%d): expanded %d raw domains into %d database domains", q.id, raw_domain_count, len(domains))


    #
    # Article domain resolution
    documents_domains = set([])
    for d in domains:
        articles_it = session.query(Article).filter_by(domain = d).options(joinedload('documents'))
        for article in articles_it:
            documents_domains.update(article.documents)
    logging.info("Query(%d): retrieved %d documents relevant to a domain", q.id, len(documents_domains))
    #
    # Identify documents containing the given keywords 
    documents_keywords = set([])
    if raw_keyword_count > 1:
        for id1, id2 in itertools.combinations(keywords, 2):
            it = session.query(KeywordAdjacency).filter_by(key1 = id1).filter_by(key2 = id2).options(joinedload('document'))
            documents_keywords.update([i.document for i in it])
    else:
        for keyword in keywords:
            it = session.query(KeywordAdjacency).filter((KeywordAdjacency.key1 == keyword) | (KeywordAdjacency.key2 == keyword)).options(joinedload('document'))
            documents_keywords.update([i.document for i in it])
    logging.info("Query(%d): retrieved %d documents relevant to a keyword", q.id, len(documents_keywords))

    #
    # Final article set resolution 
    assert using_domains or using_keywords
    documents = set([])
    if using_domains and using_keywords:
        documents = documents_keywords & documents_domains
    elif using_domains:
        documents = documents_domains 
    elif using_keywords:
        documents = documents_keywords
    logging.info("Query(%d): final document set contains %d elements", q.id, len(documents))

    #
    # Load document dependent properties
    document_ids = set([d.id for d in documents])
    documents    = set([])

    for _id in document_ids:
        document = session.query(Document).options(joinedload('*')).get(_id)
        documents.add(document)

    #
    # Date resolution 
    likely_dates = {}
    for document in documents:
        dates = {'crawled': document.parent.crawled, 'certain': set([]), 'uncertain': set([])}
        for certain_date in document.certain_dates:
            dates['certain'].add((certain_date.position, certain_date.date))
        for uncertain_date in document.uncertain_dates:
            dates['uncertain'].add((uncertain_date.position, uncertain_date.date))

        raw_input(dates)

        dates['certain'] = compute_likely_date(dates['certain'])
        dates['uncertain'] = compute_likely_date(dates['uncertain'])

        raw_input((dates, document.parent.path))