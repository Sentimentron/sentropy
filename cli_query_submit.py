#!/usr/bin/env python

import math
import itertools
import sys
import logging
import core
import time
import json
import calendar
import types
import datetime

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm import * 

from backend.db import UserQuery, UserQueryKeywordRecord, UserQueryDomainRecord, UserQueryArticleRecord
from backend.db import Keyword, Domain, KeywordAdjacency, Article, Document, KeywordIncidence, Sentence, Phrase

from collections import Counter

def mean(items):
    if len(items)  == 0:
        return 0
    if sum(items) == 0:
        return 0
    return len(items)/(1.0*sum(items))

def prepare_date(input_date):
    if type(input_date) is types.TupleType:
        return time.mktime(input_date) * 1000
    elif type(input_date) is datetime.datetime:
        return time.mktime(input_date.date().timetuple()) * 1000
    elif type(input_date) is datetime.date:
        return time.mktime(input_date.timetuple()) * 1000
    else:
        raise TypeError(type(input_date))

def compute_likely_date(date_recs, certain = False):
    ret = None 
    if certain:
        mean = 346
    else:
        mean = 307 # Something, I don't know

    logging.debug(len(date_recs))

    errors = [((p-mean)*(p-mean), j) for p, j in date_recs]
    min_error = sys.maxint
    min_value = None
    for error, rec in errors:
        if error < min_error:
            min_error = error 
            min_value = rec 

    return min_value

if __name__ == "__main__":

    core.configure_logging("debug")

    #
    # Argument processing
    query_args = sys.argv[1:]
    query_text = ' '.join(query_args)

    #
    # Database connection
    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level = 'READ COMMITTED')
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
    # OK, the approach here is to 
    # 1) Create a temporary table which can hold documents, their article id and their selection criteria [DONE]
    # 2) Copy documents with a matching domain into the temporary table  [DONE]
    # 3) Copy documents with a matching keyword into the temporary table [DONE]
    # 4) Delete records from the temporary table which don't fulfill all of the query criteria [DONE] 
    # 5) Use the document identifiers to resolve date crawled - our most basic date resolution [DONE]
    # 6) Use the document identifiers to identify the most likely certain date - our highest date resolution [DONE]
    # 7) For those documents which don't have this, compute an average uncertain date. [DONE]
    # 8) Create another temporary table which can hold phrases, copy all the phrases in all of the documents into this table 
    # 9) Filter keyword incidences by the keywords we have and join with this temporary table to count how many phrases in each document
    #    are relevant. 
    # Temporary table creation
    sql = """CREATE TEMPORARY TABLE query_%d_articles (
            id INTEGER PRIMARY KEY,
            doc_id INTEGER,
            date_certain DATE,
            date_uncertain DATE,
            date_crawled DATE,
            keywords TINYINT(1),
            domains  TINYINT(1)
        ) ENGINE=MEMORY;""" % (q.id,)
    logging.debug(sql)
    session.execute(sql)

    sql = """CREATE TEMPORARY TABLE ENGINE=MEMORY query_%d_keywords (
            id INTEGER PRIMARY KEY 
    ) ENGINE=MEMORY""" % (q.id,)
    logging.debug(sql)
    session.execute(sql)

    #
    # Article domain resolution
    documents_domains = set([]);
    for d in domains:
        sql = """INSERT INTO query_%d_articles 
        SELECT documents.article_id, documents.id, NULL, NULL, NULL, 0, 1 
        FROM documents JOIN articles ON article_id = articles.id 
        WHERE articles.domain_id = %d""" % (q.id, d.id)
        logging.debug(sql)
        session.execute(sql)
    logging.info("Query(%d): retrieved domain relevant documents", q.id)
    
    #
    # Identify documents containing the given keywords 
    documents_keywords = set([])
    if raw_keyword_count > 1:
        for key1, key2 in itertools.combinations(keywords, 2):
            sql = """INSERT INTO query_%d_articles 
            SELECT articles.id, documents.id, NULL, NULL, NULL, 1, 0 
            FROM keyword_adjacencies JOIN documents ON keyword_adjacencies.doc_id = documents.id
            JOIN articles ON documents.article_id = articles.id
            WHERE key1_id = %d AND key2_id = %d
            ON DUPLICATE KEY UPDATE keywords = 1""" % (q.id, key1_id, key2_id)
            logging.debug(sql);
            session.execute(sql);
    else:
        for keyword in keywords:
            sql = """INSERT INTO query_%d_articles 
            SELECT articles.id, documents.id, NULL, NULL, NULL, 1, 0 
            FROM keyword_adjacencies JOIN documents ON keyword_adjacencies.doc_id = documents.id
            JOIN articles ON documents.article_id = articles.id
            WHERE key1_id = %d OR key2_id = %d
            ON DUPLICATE KEY UPDATE keywords = 1""" % (q.id, keyword.id, keyword.id)
            logging.debug(sql)
            session.execute(sql)
    logging.info("Query(%d): retrieved keyword relevant documents", q.id)

    #
    # Keyword housekeeping
    for keyword in keywords: 
        _id = keyword.id 
        sql = "INSERT INTO query_%d_keywords VALUES (%d)" % (q.id, _id)
        session.execute(sql)

    #
    # Final article set resolution 
    assert using_domains or using_keywords
    sql = "DELETE FROM query_%d_articles WHERE NOT (domains = %d AND keywords = %d)" % (q.id, int(using_domains), int(using_keywords))
    logging.debug(sql)
    session.execute(sql)

    #
    # Load the documents
    documents = set([])
    sql = "SELECT doc_id FROM query_%d_articles" % (q.id, )
    for _id, in session.execute(sql):
        documents.add(session.query(Document).get(_id))

    #
    # Date resolution 
    likely_dates = {}
    sql = "SELECT articles.doc_id, articles.crawled FROM query_%d_articles JOIN articles ON query_%d_articles.id = articles.id" % (q.id, q.id)
    logging.debug(sql)
    for _id, date_crawled in session.execute(sql):
        likely_dates[_id] = ("Crawled", prepare_date(date_crawled))

    sql = """SELECT doc_id, `date` 
    FROM uncertain_dates NATURAL JOIN query_%d_articles 
    WHERE YEAR(date) > 2000 AND YEAR(date) <= 2008
    GROUP BY doc_id ORDER BY ABS(position - 307)""" % (q.id, )
    logging.debug(sql)
    for _id, date_crawled in session.execute(sql):
        likely_dates[_id] = ("Uncertain", prepare_date(date_crawled))

    sql = """SELECT doc_id, date FROM certain_dates NATURAL JOIN query_%d_articles GROUP BY doc_id ORDER BY ABS(position-346)""" % (q.id)
    logging.debug(sql)
    for _id, date_crawled in session.execute(sql):
        likely_dates[_id] = ("Certain", prepare_date(date_crawled))

    #
    # Phrase resolution 
    sql = """CREATE TEMPORARY TABLE query_%d_phrases (
            id INTEGER PRIMARY KEY,
            doc_id INTEGER,
            relevant TINYINT(1),
            prob FLOAT,
            label enum('Positive','Unknown','Negative')
        ) ENGINE=MEMORY """ % (q.id, )
    logging.debug(sql)
    session.execute(sql)

    sql = """INSERT INTO query_%d_phrases 
        SELECT phrases.id, doc_id, 0, phrases.prob, phrases.label 
        FROM query_%d_articles JOIN sentences ON doc_id = sentences.document
        JOIN phrases ON sentences.id = phrases.sentence
        WHERE phrases.label <> "Unknown"
        """  % (q.id, q.id)
    logging.debug(sql)
    session.execute(sql)

    sql = """UPDATE query_%d_phrases, keyword_incidences SET query_%d_phrases.relevant = 1 WHERE query_%d_phrases.id IN (
            SELECT phrase_id FROM keyword_incidences JOIN query_%d_keywords ON keyword_incidences.keyword_id = query_%d_keywords.id
        )""" % (q.id, q.id, q.id, q.id, q.id)

    sql = """UPDATE query_%d_phrases, (
        SELECT DISTINCT phrase_id 
        FROM keyword_incidences JOIN query_%d_keywords ON keyword_incidences.keyword_id = query_%d_keywords.id) p 
        SET relevant = 1 WHERE query_%d_phrases.id = p.phrase_id""" % (q.id, q.id, q.id, q.id)

    sql = """INSERT INTO query_%d_phrases SELECT DISTINCT phrase_id, NULL, NULL, NULL, NULL
        FROM keyword_incidences JOIN query_%d_keywords ON keyword_incidences.keyword_id = query_%d_keywords.id 
        ON DUPLICATE KEY UPDATE relevant = 1""" % (q.id, q.id, q.id)

    logging.debug(sql)
    session.execute(sql)

    sql = """SELECT COUNT(*), doc_id, AVG(prob), label FROM query_%d_phrases WHERE relevant = 1 GROUP BY doc_id, label""" % (q.id, )
    logging.debug(sql)
    document_phrase_relevance = {}
    for count, _id, prob, label in session.execute(sql):
        if _id not in document_phrase_relevance:
            document_phrase_relevance[_id] = {'pos': 0, 'neg': 0, 'prob_pos': 0, 'prob_neg': 0}

        record = document_phrase_relevance[_id]

        if label == "Positive":
            record['pos'] = count 
            record['prob_pos'] = prob
        elif label == "Negative":
            record['neg'] = count 
            record['prob_neg'] = prob 

    def generate_summary(documents, likely_dates, phrase_relevance):

        [logging.debug(x) for x in [documents, likely_dates, phrase_relevance]]

        # Create result structure
        ret = {} 

        # Compute properties for each document 
        for doc in documents:
            doc_struct = {key : None for key in ["pos_phrases", "pos_sentences", "neg_phrases", "neg_sentences", 
                "pos_phrases_rel", "neg_phrases_rel", "id", "date_method", "average_sentence_prob", "average_phrase_prob"]}
            id = doc.id 
            doc_struct["id"] = id; 
            # Check the date method
            method, date = likely_dates[id]
            doc_struct["date_method"] = method; 

            doc_struct["pos_phrases"] = doc.pos_phrases; 
            doc_struct["neg_phrases"] = doc.neg_phrases;
            doc_struct["pos_sentences"] = doc.pos_sentences;
            doc_struct["neg_sentences"] = doc.neg_sentences;

            doc_struct["pos_phrases_rel"] = phrase_relevance[id]['pos']
            doc_struct["neg_phrases_rel"] = phrase_relevance[id]['neg']
            doc_struct["average_phrase_prob"] = 0.5(phrase_relevance[id]['prob_pos'] + phrase_relevance[id]['prob_neg'])

            # Append to result structure
            if date not in ret:
                ret[date] = [] 
            ret[date].append(doc_struct)

        return ret


    #
    # General information
    logging.info("%s: Gathering information...", q);
    info = {#'articles': session.query(Article).count(),
        #'documents': session.query(Document).count(),
        #'keywords' : session.query(Keyword).count(),
        #'keyword_incidences' : session.query(KeywordIncidence).count(),
        #'keyword_bigrams': session.query(KeywordAdjacency).count(),
        #'sentences': session.query(Sentence).count(),
        #'phrases': session.query(Phrase).count(),
        "query_text": q.text,
        'domains_returned': len(domains),
        'keywords_returned': len(keywords)
    }
    logging.info("%s: Generating overall summary...", q);
    result = {
        'info': info, 
        'overview': generate_summary(documents, likely_dates, document_phrase_relevance)
    }

    for domain in domains:
        logging.info("%s: Generating summary for '%s'...", q, domain)
        subdoc = filter(lambda x: x.parent.domain == domain, documents)
        result[domain.key] = generate_summary(subdoc, likely_dates, document_phrase_relevance)

    print json.dumps(result, indent=4)


    #
    # 
