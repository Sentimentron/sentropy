#!/usr/bin/env python

import logging
import core 

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm import * 

from backend.db import UserQuery, UserQueryKeywordRecord, UserQueryDomainRecord, UserQueryArticleRecord
from backend.db import Keyword, Domain, KeywordAdjacency, Article, Document, KeywordIncidence, Sentence, Phrase

import boto.s3
import boto.sqs 
from boto.sqs.message import Message

import redis

QUERY_QUEUE_NAME = "query-queue"

class QueryQueue(object):

    def __init__(self, engine):

        self._messages = {} 
        self.queue_name = QUERY_QUEUE_NAME
        logging.info("Using '%s' as the queue.", self.queue_name)
        self._conn  = boto.sqs.connect_to_region(SQS_REGION)
        self._queue = self._conn.lookup(queue_name)
        if self._queue is None:
            logging.info("Creating '%s'...", (queue_name,))
            self._queue = self._conn.create_queue(queue_name, 120)

        logging.info("Connection established.")

    def __iter__(self):
        while 1:
            rs = sel._queue.get_messages()
            for item in rs:
                iden = int(item.get_body())
                self._messages[iden] = item 
                yield iden 

    def set_completed(self, identifier):
        logging.info("Marking %d as completed...", identifier)

        msg = self._messages[identifier]
        self._queue.delete_message(msg)
        self._messages.pop(msg, None)

class ResolutionService(object):

    def __init__(self):
        self._cache = {}

    def resolve(self, item):
        if item not in self._cache:
            return None 
        return self._cache[item]

class DatabaseResolutionService(ResolutionService):

    def __init__(self, engine):
        super(DatabaseResolutionService, self).__init__()
        self._session = Session(bind=engine)

class FuzzyDomainResolutionService(DatabaseResolutionService):

    def resolve(self, nl_domain):
        qstr = "%."+nl_domain
        for d in self._session.query(Domain).filter(Domain.key.like(qstr)):
            yield d.key

class FuzzyKeywordResolutionService(DatabaseResolutionService):

    def __init__(self, engine, query_format=None):
        super(FuzzyKeywordResolutionService, self).__init__(engine)
        if query_format is not None:
            self._query_format = query_format

    def resolve(self, nl_keyword):
        assert self._query_format is not None
        qstr = self._query_format % (nl_keyword,)
        logging.debug(qstr)

        sql = "SELECT word FROM keywords WHERE word LIKE (:qstr)";
        for k, in self._session.execute(sql, {'qstr': qstr}):
            yield k

class FuzzyKeywordCaseResolutionService(FuzzyKeywordResolutionService):
    _query_format = "%s"

class FuzzyKeywordLeftSpaceResolutionService(FuzzyKeywordResolutionService):
    _query_format = "%% %s"

class FuzzyKeywordRightSpaceResolutionService(FuzzyKeywordResolutionService):
    _query_format = "%s %%"

class FuzzyKeywordBothSpaceResolutionService(FuzzyKeywordResolutionService):
    _query_format = "%% %s %%"

class MetaResolutionService(ResolutionService):

    def __init__(self, engines):
        self.engines = engines 

class MetaComboResolutionService(MetaResolutionService):

    def resolve(self, something):
        ret = set([])
        for engine in self.engines:
            result = engine.resolve(something)
            print result
            try:
                ret.update(result)
            except TypeError, te: # Result is uniterable
                ret.add(result)

        return ret 

class MetaStackingResolutionService(MetaResolutionService):

    def resolve(self, something):
        ret = None 
        for engine in self.engines:
            ret = engine.resolve(something)
            if ret is not None:
                break 

        return ret 

class DocumentDomainResolutionService(DatabaseResolutionService):

    def resolve(self, domain_id):
        sql = """SELECT DISTINCT documents.id 
        FROM articles 
            RIGHT JOIN documents ON articles.id = documents.article_id 
        WHERE articles.domain_id = (:id)"""

        for _id, in self._session.execute(sql, {'id': domain_id}):
            yield _id 

class DocumentKeywordResolutionService(DatabaseResolutionService):

    def resolve(self, keyword_id):
        sql = """SELECT DISTINCT doc_id 
        FROM keyword_adjacencies 
        WHERE key1_id = (:id) OR key2_id = (:id)""";

        for _id, in self._session.execute(sql, {'id': keyword_id}):
            yield _id

class KeywordAdjacencyResolutionService(DatabaseResolutionService):

    def resolve(self, keyword_id, document_id):
        sql = """SELECT DISTINCT doc_id 
        FROM keyword_adjacencies
        WHERE (key1_id = (:id) OR key2_id = (:id))
        AND doc_id = (:doc)"""

        logging.debug(("KeywordAdj", keyword_id, document_id))

        for _id in self._session.execute(sql, {'id': keyword_id, 'doc': document_id}):
            return True 
        return False

class RedisResolutionService(ResolutionService):

    def __init__(self, host, port, db):
        super(RedisResolutionService, self).__init__()
        self._redis = redis.Redis(host = host, port = port, db = db)

    def resolve(self, item):
        if item in self._cache:
            return self._cache[item]

        _id = self._redis.get(item)
        if _id is not None:
            _id = int(_id)
        self._cache[item] = _id 
        return _id 

class KeywordIDResolutionService(RedisResolutionService):

    def __init__(self):
        super(KeywordIDResolutionService, self).__init__(core.get_redis_host(), 6379, 1)

class DomainIDResolutionService(RedisResolutionService):
     def __init__(self):
        super(DomainIDResolutionService, self).__init__(core.get_redis_host(), 6379, 2)

class CrawledDateResolutionService(DatabaseResolutionService):

    def resolve(self, doc_id):
        sql = """SELECT articles.crawled 
        FROM articles 
            JOIN documents ON articles.id = documents.article_id 
            WHERE documents.id=(:id)"""

        for date, in self._session.execute(sql, {'id': doc_id}):
            return "Crawled", date 

class CertainDateResolutionService(DatabaseResolutionService):

    def resolve(self, doc_id):
        sql = """SELECT certain_dates.date 
        FROM certain_dates 
        WHERE doc_id = (:id) 
        ORDER BY ABS(certain_dates.position-346) 
        LIMIT 0,1"""

        for date, in self._session.execute(sql, {'id': doc_id}):
            return "Certain", date 

class UncertainDateResolutionService(DatabaseResolutionService):

    def resolve(self, doc_id):
        sql = """SELECT uncertain_dates.date
        FROM uncertain_dates 
        WHERE doc_id = (:id)
        ORDER BY ABS(uncertain_dates.position - 307)"""

        for date, in self._session.execute(sql, {'id': doc_id}):
            return "Uncertain", date 

class DateResolutionService(MetaStackingResolutionService):

    def __init__(self, engine):
        super(DateResolutionService, self).__init__([
            e(engine) for e in [CertainDateResolutionService, UncertainDateResolutionService, CrawledDateResolutionService]])

class Phrase(object):

   def __init__(self, _id, score, prob, label):
       self.id = _id; self.score = score; self.prob = prob; self.label = label

class PhraseResolutionService(DatabaseResolutionService):

    def resolve(self, doc_id):
        sql = """SELECT phrases.id, phrases.score, phrases.prob, phrases.label
            FROM phrases JOIN sentences on phrases.sentence = sentences.id 
            WHERE sentences.document = (:id)"""
        for _id, score, prob, label in self._session.execute(sql, {'id': doc_id}):
            yield Phrase(_id, score, prob, label)

class PhraseRelevanceResolutionService(DatabaseResolutionService):

    def resolve(self, phrase_id, keyword_set):
        sql = """SELECT keyword_id 
        FROM keyword_incidences 
        WHERE phrase_id = (:id)"""
        for _id in self._session.execute(sql, {'id': phrase_id}):
            if _id in keyword_set:
                return True 
        return False

class KDQueryProcessor(object):

    def __init__(self, engine):
        self._engine = engine
        self._session = Session(bind=engine) 
        self._kres   = KeywordIDResolutionService()
        self._dres   = DomainIDResolutionService()

        self._d_res = DocumentDomainResolutionService(self._engine)
        self._k_res = DocumentKeywordResolutionService(self._engine)
        self._ka_res= KeywordAdjacencyResolutionService(self._engine)

        self._date_res   = DateResolutionService(engine)
        self._phrase_res = PhraseResolutionService(engine)
        self._phrase_res_rel = PhraseRelevanceResolutionService(engine)

    def process(self, keywords, domains):
        kwset, dmset, dset = set([]), set([]), set([])

        # Map keywords and domains to identifiers
        keywords = [(k, self._kres.resolve(k)) for k in keywords]
        domains  = [(d, self._dres.resolve(d)) for d in domains]

        dm_map = {}

        # Construct the domains set 
        for raw, d in domains:
            domains = list(self._d_res.resolve(d))
            dmset.update(domains)
            dm_map[d] = domains
            logging.debug((dmset, d))

        # Construct the final documents set
        if len(keywords) == 0:
            dset = dmset 
        else:
            for d in dmset:
                for raw, k in keywords:
                    if self._ka_res.resolve(k,d):
                        dset.add(d)
                        break 

        # Find the publication dates
        logging.info("Searching for publication dates...")
        dates = {i : self._date_res.resolve(i) for i in dset}

        # Resolve the phrases for things
        logging.info("Resolving phrases....")
        phrases = {i : [p for p in self._phrase_res.resolve(i)] for i in dset}

        # Resolve relevance 
        logging.info("Resolving phrase relevance...")
        relevance = {}
        for doc_id in phrases:
            for phrase in phrases[doc_id]:
                phrase_id = phrase.id
                relevance[phrase_id] = self._phrase_res_rel.resolve(phrase_id, keywords)

        # Return the documents
        dset = [self._session.query(Document).get(_id) for _id in dset]

        raw_input("keywords"); raw_input(keywords)
        raw_input("domains"); raw_input(dm_map)
        raw_input("dset"); raw_input(dset)
        raw_input("dates"); raw_input(dates);
        raw_input("phrases"); raw_input(phrases);
        raw_input("relevance"); raw_input(relevance);

        return keywords, domains, dm_map, dset, dates, phrases, relevance


def present(keywords, using_keywords, domains, dmap, dset, dates, phrases, relevance, query_text):

    import csv
    import StringIO

    ret = {}
    info = {}
    info['query_time'] = 0
    info['keywords_returned'] = len(keywords)
    info['result_version'] = 2
    info['using_keywords'] = int(len(keywords)>0)
    info['documents_returned'] = len(dset)
    info['domains_returned'] = len(dmap)
    info['query_text'] = query_text
    info['domains'] = dmap 
    info['phrases_returned'] = len(phrases)
    info['keyword_set'] = [raw for raw, k in keywords]

    overview = {}

    fp = StringIO.StringIO()
    wr = csv.writer(fp)

    for doc in dset:
        method, date = dates[doc.id]
        pos_rel_phrases = len([d for d in relevance[doc.id] if d.label == "Positive"])
        neg_rel_phrases = len([d for d in relevance[doc.id] if d.label == "Negative"])
        prob_phrases = 0
        row = [doc.id, method, date, 
            doc.pos_phrases, doc.neg_phrases, doc.pos_phrases, doc.pos_sentences, 
            doc.neg_sentences, pos_rel_phrases, neg_rel_phrases
        ]
        wr.writerow(row)

    print fp.getvalue()


if __name__ == "__main__":
    core.configure_logging('debug')
    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level = 'READ UNCOMMITTED')

    kdproc  = KDQueryProcessor(engine)
    fd      = FuzzyDomainResolutionService(engine)
    kwstack = MetaComboResolutionService([k(engine, None) for k in [FuzzyKeywordCaseResolutionService]])

    if "--cli" in sys.argv:
        query = raw_input("Enter query:")
        uq    = UserQuery(query)
        domains = set([])
        keywords = set([])
        for domain in uq.get_domains(query):
            domains.add(domain)
            domains.update(fd.resolve(domain))
            logging.info((domain, domains))

        for keyword in uq.get_keywords(query):
            keywords.add(keyword)
            keywords.update(kwstack.resolve(keyword))
            logging.info((keyword, keywords))

        keywords, domains, dmap, dset, dates, phrases, relevance = kdproc.process(keywords, domains)
        result = present(keywords, len(keywords)> 0, domains, dmap, dset, dates, phrases, relevance)
