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
    start = datetime.datetime(year=1970,month=1,day=1)
    diff = input_date - start
    return int(diff.total_seconds()*1000)

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
            doc_id INTEGER NOT NULL,
            domain_id INTEGER,
            keywords TINYINT(1) NOT NULL,
            domains  TINYINT(1) NOT NULL
        ) ENGINE=MEMORY;""" % (q.id,)
    logging.debug(sql)
    session.execute(sql)

    sql = """CREATE TEMPORARY TABLE query_%d_keywords (
            id INTEGER PRIMARY KEY 
    ) ENGINE=MEMORY""" % (q.id,)
    logging.debug(sql)
    session.execute(sql)

    #
    # Keyword housekeeping
    for keyword in keywords: 
        _id = keyword.id 
        sql = "INSERT INTO query_%d_keywords VALUES (%d)" % (q.id, _id)
        session.execute(sql)

    if using_keywords and not using_domains:
        sql = """SELECT articles.domain_id, COUNT(*) as Count FROM articles JOIN documents ON articles.id = documents.article_id JOIN 
            keyword_adjacencies ON keyword_adjacencies.doc_id = documents.id, query_%d_keywords  
            WHERE query_%d_keywords.id = keyword_adjacencies.key1_id OR query_%d_keywords.id = keyword_adjacencies.key2_id
            GROUP BY articles.domain_id
            ORDER BY Count DESC LIMIT 0,5""" % (q.id, q.id, q.id)
        logging.debug(sql);
        for domain_id, count in session.execute(sql):
            logging.debug("Consolidated: Domain(%d) (%d)", domain_id, count)
            domain = session.query(Domain).get(domain_id)
            domains.add(domain)
            raw_domain_count += 1
            using_domains = True 

    #
    # Article domain resolution
    documents_domains = set([]);
    for d in domains:
        sql = """INSERT INTO query_%d_articles 
        SELECT documents.article_id, documents.id, articles.domain_id, 0, 1 
        FROM documents JOIN articles ON article_id = articles.id 
        WHERE articles.domain_id = %d""" % (q.id, d.id)
        logging.debug(sql)
        session.execute(sql)
    logging.info("Query(%d): retrieved domain relevant documents", q.id)
    
    #
    # Identify documents containing the given keywords 
    documents_keywords = set([])

    _q_condition = "AND"
    if raw_keyword_count == 1:
        _q_condition = "OR"

    _kw_list = []
    if raw_keyword_count > 1:
        for key1, key2 in itertools.combinations(keywords, 2):
            _kw_list.append((key1.id, key2.id))
    else:
        for keyword in keywords:
            _kw_list.append((keyword.id, keyword.id))

    if using_domains:
        sql = """CREATE TEMPORARY TABLE query_%d_article_ids (
                id INTEGER PRIMARY KEY 
        ) ENGINE=MEMORY""" % (q.id,)
        logging.debug(sql)
        session.execute(sql)

        sql = """INSERT INTO query_%d_article_ids SELECT id FROM query_%d_articles"""  % (q.id, q.id)
        logging.debug(sql)
        session.execute(sql)
        _article_source = "query_%d_article_ids" % (q.id,)
    else:
        _article_source = "articles"

    for key1, key2 in _kw_list:
        sql = """INSERT INTO query_%d_articles
            SELECT %s.id, documents.id, NULL, 1, 0
                FROM keyword_adjacencies RIGHT JOIN documents ON keyword_adjacencies.doc_id = documents.id
                RIGHT JOIN %s ON documents.article_id = %s.id 
                WHERE key1_id = %d %s key2_id = %d
                ON DUPLICATE KEY UPDATE keywords = 1""" % (q.id, _article_source, _article_source, _article_source, key1, _q_condition, key2)

        logging.debug(sql)
        session.execute(sql)

    if False:
        if raw_keyword_count > 1:
            for key1, key2 in itertools.combinations(keywords, 2):
                sql = """INSERT INTO query_%d_articles 
                SELECT articles.id, documents.id, NULL, 1, 0 
                FROM keyword_adjacencies RIGHT JOIN documents ON keyword_adjacencies.doc_id = documents.id
                RIGHT JOIN articles ON documents.article_id = articles.id 
                WHERE key1_id = %d AND key2_id = %d
                ON DUPLICATE KEY UPDATE keywords = 1""" % (q.id, key1_id, key2_id)
                logging.debug(sql);
                session.execute(sql);
        else:
            for keyword in keywords:
                sql = """INSERT INTO query_%d_articles 
                SELECT articles.id, documents.id, NULL, 1, 0 
                FROM keyword_adjacencies RIGHT JOIN documents ON keyword_adjacencies.doc_id = documents.id
                RIGHT JOIN articles ON documents.article_id = articles.id
                WHERE key1_id = %d OR key2_id = %d
                ON DUPLICATE KEY UPDATE keywords = 1""" % (q.id, keyword.id, keyword.id)
                logging.debug(sql)
                session.execute(sql)
    logging.info("Query(%d): retrieved keyword relevant documents", q.id)

    #
    # Final article set resolution 
    assert using_domains or using_keywords
    sql = "DELETE FROM query_%d_articles WHERE NOT (domains = %d AND keywords = %d)" % (q.id, int(using_domains), int(using_keywords))
    logging.debug(sql)
    session.execute(sql)

    #
    # Load the documents
    documents = set([])
    document_domain_mapping = {}
    document_id_mapping = {}
    sql = """SELECT documents.id, documents.article_id, documents.length, documents.label, 
        documents.headline, documents.pos_phrases, documents.neg_phrases, documents.pos_sentences,
        documents.neg_sentences, query_%d_articles.domain_id FROM query_%d_articles LEFT JOIN documents ON query_%d_articles.doc_id = documents.id""" % (q.id, q.id, q.id)
    
    for _id, article_id, length, label, headline, pos_phrases, neg_phrases, pos_sentences, neg_sentences, domain in session.execute(sql):

        if domain is not None:
            if domain not in document_domain_mapping:
                document_domain_mapping[domain] = set([])
            document_domain_mapping[domain].add(_id)

        if label == "Positive":
            label =  1
        elif label == "Negative":
            label = -1
        else:
            label = 0
        
        d = Document(article_id, label, length, pos_sentences, neg_sentences, pos_phrases, neg_phrases, headline)
        d.id = _id 
        documents.add(d)
        document_id_mapping[d.id] = d

        logging.info("Loaded document %d", d.id)


    #
    # Date resolution 
    likely_dates = {}
    sql = "SELECT doc_id, articles.crawled FROM query_%d_articles LEFT JOIN articles ON query_%d_articles.id = articles.id" % (q.id, q.id)
    logging.debug(sql)
    for _id, date_crawled in session.execute(sql):
        likely_dates[_id] = ("Crawled", prepare_date(date_crawled))

    sql = """SELECT uncertain_dates.doc_id, `date` 
    FROM uncertain_dates JOIN query_%d_articles ON uncertain_dates.doc_id = query_%d_articles.doc_id
    WHERE YEAR(date) > 2000 AND YEAR(date) <= 2009
    GROUP BY doc_id ORDER BY ABS(position - 307)""" % (q.id, q.id)
    logging.debug(sql)
    for _id, date_crawled in session.execute(sql):
        likely_dates[_id] = ("Uncertain", prepare_date(date_crawled))

    sql = """SELECT certain_dates.doc_id, `date` 
    FROM certain_dates JOIN query_%d_articles ON query_%d_articles.doc_id = certain_dates.doc_id 
    WHERE YEAR(date) > 2000 AND YEAR(date) <= 2009
    GROUP BY doc_id 
    ORDER BY ABS(position-346)""" % (q.id, q.id)
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
        FROM query_%d_articles LEFT JOIN sentences ON doc_id = sentences.document
        LEFT JOIN phrases ON sentences.id = phrases.sentence
        WHERE phrases.label <> "Unknown"
        """  % (q.id, q.id)
    logging.debug(sql)
    session.execute(sql)

    sql = """INSERT INTO query_%d_phrases SELECT DISTINCT phrase_id, NULL, NULL, NULL, NULL
        FROM keyword_incidences RIGHT JOIN query_%d_keywords ON keyword_incidences.keyword_id = query_%d_keywords.id 
        ON DUPLICATE KEY UPDATE relevant = 1""" % (q.id, q.id, q.id)

    logging.debug(sql)
    session.execute(sql)

    sql = """SELECT COUNT(*), doc_id, AVG(prob), label FROM query_%d_phrases WHERE relevant = 1 GROUP BY doc_id, label""" % (q.id, )
    logging.debug(sql)
    document_phrase_relevance = {_id : {'pos': 0, 'neg': 0, 'prob_pos': 0, 'prob_neg': 0} for _id in [d.id for d in documents]}
    for count, _id, prob, label in session.execute(sql):
        record = document_phrase_relevance[_id]
        if label == "Positive":
            record['pos'] = count 
            record['prob_pos'] = prob
        elif label == "Negative":
            record['neg'] = count 
            record['prob_neg'] = prob 

    def generate_domain_summary(domain, session):
        # Phase 1: generate statistics for the sites a domain links to 
        total_self_links, total_external_links = 0, 0
        external_links = {}
        sql = """SELECT domains.`key`, COUNT(*) AS c FROM articles 
            JOIN documents ON articles.id = documents.article_id
            JOIN links_absolute ON links_absolute.document_id = documents.id 
            JOIN domains ON links_absolute.domain_id = domains.id 
            WHERE articles.domain_id = %d
            GROUP BY links_absolute.domain_id""" % (domain.id)
        logging.debug(sql)
        for key, count in session.execute(sql):
            if key == domain.key:
                total_self_links += count
            else:
                total_external_links += count
                external_links[key] = count 

        # Phase 2: generate statistics for internal links (coverage)
        internal = set([])
        covered  = set([])
        # Get all of the internal links
        sql = """SELECT links_relative.path 
        FROM articles JOIN documents ON articles.id = documents.article_id 
        JOIN links_relative ON links_relative.document_id = documents.id 
        WHERE articles.domain_id = %d AND LENGTH(links_relative.path) > 1;""" % (domain.id)
        logging.debug(sql)

        for path, in session.execute(sql):
            internal.add(path)

        # Get all of the articles
        sql = """SELECT articles.path 
        FROM articles
        WHERE articles.domain_id = %d""" % (domain.id)
        logging.debug(sql)
        for path, in session.execute(sql):
            covered.add(path)

        cf = 100*len(covered.intersection(internal)) / len(covered.union(internal))

        # Phase 3: Get a list of keywords the site often talks about 
        sql = """SELECT count(*) AS c, doc_id, key1_id, keywords1.word, key2_id, keywords2.word FROM keyword_adjacencies 
        JOIN keywords AS keywords1 ON keywords1.id = key1_id 
        JOIN keywords AS keywords2 ON keywords2.id = key2_id 
        JOIN documents ON keyword_adjacencies.doc_id = documents.id 
        JOIN articles ON articles.id = documents.article_id 
        WHERE articles.domain_id = %d 
        GROUP BY key1_id, key2_id 
        ORDER BY c DESC LIMIT 0,50;""" % (domain.id)
        logging.debug(sql)
        count_tracking = set([])
        terms = set([])
        word_forms = {}
        for c, doc_id, key1, word1, key2, word2 in session.execute(sql):
            if c in count_tracking:
                continue 
            word1, word2 = [x.lower() for x in [word1, word2]]
            #terms.add(word1.lower() + " " + word2.lower())
            if word1 in word_forms:
                form = word_forms[word1]
                form.append(word2)
                word_forms.pop(word1, None)
                word_forms[word2] = form
            else:
                word_forms[word2] = [word1, word2]

        terms = [' '.join(word_forms[w]) for w in word_forms]

        return external_links, cf, terms


    def generate_summary(documents, likely_dates, phrase_relevance):

        #[logging.debug(x) for x in [documents, likely_dates, phrase_relevance]]

        # Create result structure
        ret = {} 

        # Compute properties for each document 
        for doc in documents:
            doc_struct = {key : None for key in ["pos_phrases", "pos_sentences", "neg_phrases", "neg_sentences", 
                "pos_phrases_rel", "neg_phrases_rel", "id", "date_method", "average_phrase_prob", "label"]}
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
            doc_struct["average_phrase_prob"] = round(0.5*(phrase_relevance[id]['prob_pos'] + phrase_relevance[id]['prob_neg']), 2);

            label = None
            if doc.label == "Negative":
                label = -1;
            elif doc.label == "Positive":
                label = 1
            else:
                label = 0
            doc_struct["label"] = label

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
        'keywords_returned': len(keywords),
        'using_keywords': int(using_keywords),
        'result_version': '1'
    }
    logging.info("%s: Generating overall summary...", q);
    result = {
        'info': info, 
        'overview': generate_summary(documents, likely_dates, document_phrase_relevance),
        'details': {}
    }

    for domain in domains:
        _id = domain.id
        if _id not in document_domain_mapping:
            continue
        logging.debug(document_id_mapping.keys())
        logging.debug(document_domain_mapping.keys())
        subdoc_ids = document_domain_mapping[_id]
        subdocs  = [document_id_mapping[s] for s in subdoc_ids]

        logging.info("%s: Generating summary for '%s'...", q, domain)
        record = {}
        result['details'][domain.key] = record

        record['docs'] = generate_summary(subdocs, likely_dates, document_phrase_relevance)
        logging.info("%s: Getting additional info for '%s'...", q, domain)
        external, cf, terms = generate_domain_summary(domain, session)
        record['external'] = external 
        record['coverage'] = round(cf)
        record['terms'] = terms


    print json.dumps(result, indent=4)


    #
    # 
