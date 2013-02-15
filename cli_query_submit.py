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

    core.configure_logging()

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
    # Temporary table creation
    sql = """CREATE TEMPORARY TABLE query_%d_articles (
            id INTEGER PRIMARY KEY,
            doc_id INTEGER,
            date_certain DATE,
            date_uncertain DATE,
            date_crawled DATE,
            keywords TINYINT(1),
            domains  TINYINT(1)
        );""" % (q.id,)
    logging.debug(sql)
    session.execute(sql)

    #
    # Article domain resolution
    documents_domains = set([]);
    for d in domains:
        sql = """INSERT INTO query_%d_articles 
        SELECT article_id, id, NULL, NULL, NULL, 0, 1 
        FROM documents JOIN articles ON article_id = articles.id 
        WHERE articles.domain_id = %d""" % (q.id, d.id)
        logging.debug(sql)
        session.execute(sql)
    logging.info("Query(%d): retrieved domain relevant documents", q.id, len(documents_domains))
    
    #
    # Identify documents containing the given keywords 
    documents_keywords = set([])
    if raw_keyword_count > 1:
        for key1, key2 in itertools.combinations(keywords, 2):
            sql = """INSERT INTO query_%d_articles 
            SELECT documents.article_id, documents.id, NULL, NULL, NULL, 1, 0 
            FROM documents JOIN articles ON documents.article_id = articles.id 
            WHERE documents.id IN (SELECT doc_id FROM keyword_adjacencies WHERE key1_id = %d AND key2_id = %d)
            ON DUPLICATE KEY UPDATE keywords = 1""" % (q.id, key1.id, key2.id)
            logging.debug(sql);
            session.execute(sql);
    else:
        for keyword in keywords:
            sql = """INSERT INTO query_%d_articles 
            SELECT documents.article_id, documents.id, NULL, NULL, NULL, 1, 0 
            FROM documents JOIN articles ON documents.article_id = articles.id 
            WHERE documents.id IN (SELECT doc_id FROM keyword_adjacencies WHERE key1_id = %d OR key2_id = %d)
            ON DUPLICATE KEY UPDATE keywords = 1""" % (q.id, keyword.id, keyword.id)
            logging.debug(sql)
            session.execute(sql)
    logging.info("Query(%d): retrieved %d documents relevant to a keyword", q.id, len(documents_keywords))

    #
    # Final article set resolution 
    assert using_domains or using_keywords
    documents = set([])

    for id, in session.execute("SELECT doc_id FROM query_%d_articles" % (q.id, )):
        documents.add(id)
    logging.info("Query(%d): final document set contains %d elements", q.id, len(documents))

    #
    # Load document dependent properties
    document_ids = set([d.id for d in documents])
    documents    = set([])

    for _id in document_ids:
        document = session.query(Document).get(_id)
        documents.add(document)

    logging.info("Finished loading final document set.")

    #
    # Date resolution 
    likely_dates = {}
    date_methods = Counter()
    for document in documents:
        dates = {'certain': set([]), 'uncertain': set([])}
        for certain_date in document.certain_dates: 
            dates['certain'].add((certain_date.position, prepare_date(certain_date.date)))
        for uncertain_date in document.uncertain_dates:
            dates['uncertain'].add((uncertain_date.position, prepare_date(uncertain_date.date)))

        dates['certain'] = compute_likely_date(dates['certain'])
        dates['uncertain'] = compute_likely_date(dates['uncertain'], False)

        if dates["certain"] is not None:
            likely_dates[document.id] = ("Certain", dates["certain"])
            date_methods.update(["Certain"])
        elif dates["uncertain"] is not None:
            likely_dates[document.id] = ("Uncertain", dates["uncertain"])
            date_methods.update(["Uncertain"])
        else:
            likely_dates[document.id] = ("Crawled", prepare_date(document.parent.crawled.date()))
            date_methods.update(["Crawled"])

        logging.info("Query(%d): resolved dates (Certain: %d, Uncertain: %d, Crawled %d)", q.id,\
            date_methods["Certain"], date_methods["Uncertain"], date_methods["Crawled"])

    logging.info("Finished resolving dates.")

    def generate_summary(documents, likely_dates, keywords):

        # Create result structure
        ret = {} #{date: [] for method, date in [likely_dates[i] for i in likely_dates]}

        # Just clarify what we're looking for
        keywords = [k.id for k in keywords]

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

            # Compute average probabilities and the number of relevant phrases
            sentence_probs = [] 
            phrase_probs   = []
            relevant_phrase_pos, relevant_phrase_neg = 0, 0
            for sentence in doc.sentences:
                if sentence.label != "Unknown":
                    sentence_probs.append(sentence.prob) # TODO: need to verify this field 
                for phrase in sentence.phrases:
                    if phrase.label != "Unknown":
                        phrase_probs.append(phrase.prob)
                        it = [i for i in phrase.keyword_incidences if i.keyword_id in keywords]
                        if len(it) == 0:
                            continue
                        if phrase.label == "Positive":
                            relevant_phrase_pos += 1
                        if phrase.label == "Negative":
                            relevant_phrase_neg += 1

            doc_struct["pos_phrases_rel"] = relevant_phrase_pos
            doc_struct["neg_phrases_rel"] = relevant_phrase_neg 

            doc_struct["average_sentence_prob"] = mean(sentence_probs)
            doc_struct["average_phrase_prob"]   = mean(phrase_probs  )

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
        'overview': generate_summary(documents, likely_dates, keywords)
    }

    for domain in domains:
        logging.info("%s: Generating summary for '%s'...", q, domain)
        subdoc = filter(lambda x: x.parent.domain == domain, documents)
        result[domain.key] = generate_summary(subdoc, likely_dates, keywords)

    print json.dumps(result, indent=4)


    #
    # 
