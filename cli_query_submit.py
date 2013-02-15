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
            likely_dates[document.id] = dates["certain"]
            date_methods.update(["Certain"])
        elif dates["uncertain"] is not None:
            likely_dates[document.id] = dates["uncertain"]
            date_methods.update(["Uncertain"])
        else:
            likely_dates[document.id] = prepare_date(document.parent.crawled.date())
            date_methods.update(["Crawled"])

        logging.info("Query(%d): resolved dates (Certain: %d, Uncertain: %d, Crawled %d)", q.id,\
            date_methods["Certain"], date_methods["Uncertain"], date_methods["Crawled"])

    logging.info("Finished resolving dates.")

    def generate_summary(documents, likely_dates, keywords):

        # Create result structure
        ret = {date: [] for date in [likely_dates[i] for i in likely_dates]}

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
                        it = session.query(KeywordIncidence).filter_by(phrase_id = phrase.id).filter(KeywordIncidence.keyword_id._in(keywords))
                        if it.count() == 0:
                            continue 
                        if phrase.label == "Positive":
                            relevant_phrase_pos += 1
                        if phrase.label == "Negative":
                            relevant_phrase_neg += 1

            doc_struct["average_sentence_prob"] = mean(doc_struct["average_sentence_prob"])
            doc_struct["average_phrase_prob"]   = mean(doc_struct["average_phrase_prob"  ])

            # Append to result structure 
            ret[date].append(doc_struct)

        return ret


    #
    # General information
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

    result = {
        'info': info, 
        'overview': generate_summary(documents, likely_dates)
    }

    for domain in domains:
        subdoc = filter(lambda x: x.parent.domain == domain, documents)
        result[domain.key] = generate_summary(subdoc, likely_dates)

    print json.dumps(result, indent=4)


    #
    # 