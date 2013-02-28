#!/usr/bin/env python

import logging
import core
import datetime 

from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from sqlalchemy.orm.exc import *
from sqlalchemy.orm import * 

from backend.db import UserQuery, UserQueryKeywordRecord, UserQueryDomainRecord, UserQueryArticleRecord
from backend.db import Keyword, Domain, KeywordAdjacency, Article, Document, KeywordIncidence, Sentence, Phrase

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto.s3
import boto.sqs 
from boto.ses.connection import SESConnection

from sqlalchemy.sql.functions import now
from boto.s3.key import Key
from boto.sqs.message import Message
SQS_REGION = "us-east-1"

import redis
FOOTER = """
Sentimentron has removed your email from its database.
--
Copyright &copy; 2012 Richard Townsend. Sentimentron is a research project, results are automatically generated and may not be accurate. 
<a href=\"http://www.sentimentron.co.uk/terms.html\">Terms of use</a>"""
QUERY_QUEUE_NAME = "query-queue"

def prepare_date(input_date):
    start = datetime.datetime(year=1970,month=1,day=1)
    diff = input_date - start
    return int(diff.total_seconds()*1000)

class QueryMessage(object):

    def __init__(self, message):
        self.message = message 

    def __str__(self):
        return self.message 

class EmailProcessor(object):

    def __init__(self):
        self.con = SESConnection()

    def send_success(self, to, id):
        msg = """Hello! This is an automated email from Sentimentron.

Sentimentron's finished processing your request and has produced some results. 
To view them, copy the following into your browser:

    <ul style="list-style-type:none"><li><a href="http://results.sentimentron.co.uk/index.html#%d">http://results.sentimentron.co.uk/index.html#%d</a></li></ul>

Hope you find the results useful! If you have any questions or feedback, please email <a href="mailto:feeback@sentimentron.co.uk">feedback@sentimentron.co.uk</a>.""" % (id, id)
    
        self.con.send_email("no-reply@sentimentron.co.uk", 
            "Good news from Sentimentron",
            msg + FOOTER,
            [to], None, None, 'html'
        )

    def send_failure(self, to, error):
        msg = """Hello! This is an automated email from Sentimentron.

Unfortunately, Sentimentron encountered a problem processing your query and
hasn't produced any results. Apologies for the inconvenience. The problem was:

    <ul style="list-style-type:none; font-weight:bold"><li>%s</li></ul>

If you have any questions or feedback, please email <a href="mailto:feeback@sentimentron.co.uk">feedback@sentimentron.co.uk</a>.""" % (error,)

        self.con.send_email('no-reply@sentimentron.co.uk', 
            'Bad news from Sentimentron',
            msg + FOOTER,
            [to], None, None, 'html'
        )

class QueryException(Exception):

    def __init__(self, message):
        self.message = message

class QueryQueue(object):

    def __init__(self, engine):

        self._messages = {} 
        self.queue_name = QUERY_QUEUE_NAME
        logging.info("Using '%s' as the queue.", self.queue_name)
        self._conn  = boto.sqs.connect_to_region(SQS_REGION)
        self._queue = self._conn.lookup(QUERY_QUEUE_NAME)
        if self._queue is None:
            logging.info("Creating '%s'...", (QUERY_QUEUE_NAME,))
            self._queue = self._conn.create_queue(QUERY_QUEUE_NAME, 120)

        logging.info("Connection established.")

    def __iter__(self):
        while 1:
            rs = self._queue.get_messages()
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

class StrictKeywordAdjacencyResolutionService(DatabaseResolutionService):

    def resolve(self, key1_id, key2_id, document_id):
        sql = """SELECT DISTINCT doc_id 
        FROM keyword_adjacencies
        WHERE (key1_id = (:id1) AND key2_id = (:id2))
        AND doc_id = (:doc)"""

        logging.debug(("StrictKeywordAdj", key1_id, key2_id, document_id))
        for _id in self._session.execute(sql, {'id1': key1_id, 'id2':key2_id, 'doc':document_id}):
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

class DateResolutionService(DatabaseResolutionService):
    @classmethod 
    def present_date(cls, input_date):
        start = datetime.datetime(year=1970,month=1,day=1)
        diff = input_date - start
        return int(diff.total_seconds()*1000)

class CrawledDateResolutionService(DateResolutionService):

    def resolve(self, doc_id):
        sql = """SELECT articles.crawled 
        FROM articles 
            JOIN documents ON articles.id = documents.article_id 
            WHERE documents.id=(:id)"""

        for date, in self._session.execute(sql, {'id': doc_id}):
            return "Crawled", date 

class CertainDateResolutionService(DateResolutionService):

    def resolve(self, doc_id):
        sql = """SELECT certain_dates.date 
        FROM certain_dates 
        WHERE doc_id = (:id) 
        ORDER BY ABS(certain_dates.position-346) 
        LIMIT 0,1"""

        for date, in self._session.execute(sql, {'id': doc_id}):
            return "Certain", date 

class UncertainDateResolutionService(DateResolutionService):

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
        for _id, in self._session.execute(sql, {'id': phrase_id}):
            if _id in keyword_set:
                return True 
        return False

class KQueryProcessor(object):

    def __init__(self, engine):
        self._kd_proc = KDQueryProcessor(engine)
        self._kres = KeywordIDResolutionService()

    def get_document_rows(self, keywords, domains=set([]), dmset = set([])):
        # Create a new session
        session = Session(bind = engine)

        # Look up the article keywords
        kres = KeywordIDResolutionService()
        _keywords = {k : self._kres.resolve(k) for k in keywords}

        # Find the sites which talk about a particular keyword 
        sql = """ SELECT domains.`key`, COUNT(*) AS c from domains JOIN articles ON articles.domain_id = domains.id 
            JOIN documents ON documents.article_id = articles.id 
            JOIN keyword_adjacencies ON keyword_adjacencies.doc_id = documents.id 
            WHERE keyword_adjacencies.key1_id IN (:keys)
            OR keyword_adjacencies.key2_id IN (:keys)
            GROUP BY domains.id 
            ORDER BY c DESC 
            LIMIT 0,5
        """
        for key, count in session.execute(sql, ({'keys': ','.join([str(i) for i in _keywords.values()])})):
            logging.info((key, count))
            domains.add(key)

        return self._kd_proc.get_document_rows(keywords, domains, dmset)

class KDQueryProcessor(object):

    def __init__(self, engine):
        self._engine = engine
        self._session = Session(bind=engine) 
        self._kres   = KeywordIDResolutionService()
        self._dres   = DomainIDResolutionService()

        self._d_res = DocumentDomainResolutionService(self._engine)
        self._k_res = DocumentKeywordResolutionService(self._engine)
        self._ka_res= KeywordAdjacencyResolutionService(self._engine)
        self._ska_res = StrictKeywordAdjacencyResolutionService(self._engine)

        self._date_res   = DateResolutionService(engine)
        self._phrase_res = PhraseResolutionService(engine)
        self._phrase_res_rel = PhraseRelevanceResolutionService(engine)

    def get_document_rows(self, keywords, domains, dmset = set([])):
        import itertools
        using_bigrams = False
        kwset,  dset = set([]), set([])

        # Map keywords and domains to identifiers
        keywords = {k : self._kres.resolve(k) for k in keywords}
        domains  = {d : self._dres.resolve(d) for d in domains}

        resolved = 0
        for k in keywords:
            if keywords[k] is None:
                yield QueryMessage("Couldn't find any keywords matching '%s'" % (k,))
            else:
                resolved += 1
        if resolved == 0:
            raise QueryException("No matching keywords.")

        resolved = 0
        for d in domains:
            if domains[d] is None:
                yield QueryMessage("Couldn't find any domains matching '%s'" % (d,))
            else:
                resolved += 1
        if resolved == 0:
            raise QueryException("No matching domains.")

        logging.debug(keywords); logging.debug(domains)

        # Construct the domains set 
        for raw in domains:
            domain_contents = list(self._d_res.resolve(domains[raw]))
            if len(domain_contents) == 0:
                yield QueryMessage("No documents found for domain %s", (raw,))
            dmset.update([(d, raw) for d in domain_contents])

        # Generate list of possible keyword bigrams
        bigram_gen = [(keywords[x], keywords[y]) for x, y in itertools.combinations(keywords, 2)]
        logging.debug(bigram_gen)
        for d, raw_domain in dmset: 
            added = False 
            for key1, key2 in bigram_gen:
                if self._ska_res.resolve(key1, key2, d):
                    added = True 
                    logging.debug((key1, key2, d))
                    dset.add((d, raw_domain)) 
                    break
            if added:
                continue

        using_bigrams = len(dset) > 100
        if not using_bigrams:
            yield QueryMessage("Few exact matches for this query. Expanding...")
            # Construct the final documents set
            if len(keywords) == 0:
                dset = dmset 
            else:
                for d, raw_domain in dmset:
                    for raw in keywords:
                        k = keywords[raw]
                        if self._ka_res.resolve(k,d):
                            dset.add((d, raw_domain))
                            break 

        yield QueryMessage("Fetching document details...")
        for d, raw_domain in dset:
            logging.info("%d Fetching document details", d)
            doc = self._session.query(Document).get(d)

            logging.info("%d Searching for publication dates...", d)
            method, date = self._date_res.resolve(d)

            logging.info("%d Resolving phrases...")
            pos, neg = 0, 0
            relevant_pos, relevant_neg = 0, 0
            phrases = self._phrase_res.resolve(d)

            phrase_prob_total, phrase_count = 0, 0

            for p in phrases:
                phrase_count += 1
                phrase_prob_total += p.prob
                if self._phrase_res_rel.resolve(p.id, keywords):
                    if p.label == "Positive":
                        relevant_pos += 1
                    elif p.label == "Negative":
                        relevant_neg += 1

            yield [
                doc.id, raw_domain, method, date, 
                doc.pos_phrases, doc.neg_phrases, doc.pos_sentences, 
                doc.neg_sentences, relevant_pos, relevant_neg,
                doc.label, phrase_prob_total
            ]


class ResultPresenter(object):

    def __init__(self, keywords, query_text):
        self.keywords = keywords
        self.query_text = query_text

    def add_result(self, id, domain, method, date, pos_phrases, neg_phrases, pos_sentences, neg_sentences, relevant_pos, relevant_neg, label, phrase_prob):
        pass 

    def present(self):
        pass 

class JSONResultPresenter(ResultPresenter):

    def __init__(self, keywords, query_text, engine):
        super(JSONResultPresenter, self).__init__(keywords, query_text)

        info                       = {}
        info['keywords_returned' ] = len(keywords)
        info['result_version'    ] = 2
        info['using_keywords'    ] = int(len(keywords)>0)
        info['documents_returned'] = 0
        info['query_text'        ] = self.query_text
        info['phrases_returned'  ] = 0
        info['sentences_returned'] = 0
        info['documents_returned'] = 0
        info['keywords_set'      ] = list(keywords)

        self.info     = info
        self.response = {'info': info, 'siteData': {}}
        self.dset     = set([])

        self._session = Session(bind = engine)

    @classmethod 
    def convert_doc_label(cls, label):
        if label == "Negative":
            return -1
        elif label == "Positive":
            return 1
        return 0

    @classmethod
    def convert_date(cls, in_date):
        start = datetime.datetime(year=1970, month=1, day=1)
        diff  = in_date - start
        return int(diff.total_seconds() * 1000)

    @classmethod
    def convert_method(cls, method):
        ret = -1
        if method == "Certain":
            ret = 0
        elif method == "Uncertain":
            ret = 1
        elif method == "Crawled":
            ret = 2

        if ret == -1:
            raise ValueError(method)

        return ret

    def add_result(self, id, domain, method, date, pos_phrases, neg_phrases, pos_sentences, neg_sentences, relevant_pos, relevant_neg, label, phrase_prob):
        # Add new domain if needed
        if domain not in self.response['siteData']:
            self.response['siteData'][domain] = {'docs': [], 'details': {}}

        # Result presentation
        label       = self.convert_doc_label(label)
        date        = self.convert_date(date)
        phrase_prob = round(phrase_prob, 2)
        method      = self.convert_method(method)

        # Domain record 
        record = self.response['siteData'][domain]['docs']
        record.append([method, date, pos_phrases, neg_phrases, pos_sentences, neg_sentences, relevant_pos, relevant_neg, label, phrase_prob, id])

        self.dset.add((id, domain))

        # Misc record 
        self.info['sentences_returned'] += pos_sentences + neg_sentences
        self.info['phrases_returned'  ] += pos_phrases   + neg_phrases
        self.info['documents_returned'] += 1

    def additional(self):
        from collections import Counter
        import random
        ret = {}
        # Collect statistics
        for doc_id, domain in self.dset:
            if domain not in ret:
                ret[domain] = {'external': Counter(), 'keywords': Counter([]), 'known': set([]), 'all': set([])}
            else:
                continue
            record = ret[domain]
            doc = self._session.query(Document).get(doc_id)
            article = doc.parent
            record['known'].add(article)

            # Phase 1: relative links to site pages
            logging.info("Resolving internal links for %d in %s", doc_id, domain)
            for link in doc.relative_links:
                path = link.path.partition('#')[0]
                # TODO: deal with relative paths
                it   = self._session.query(Article).filter_by(domain_id = article.domain_id, path = path)
                record['all'].update(it)
                record['external'][domain] += it.count()

            # Phase 2: absolute links to other articles
            logging.info("Absolute links for %d in %s", doc_id, domain)

            for link in doc.absolute_links:
                if link.domain_id == article.domain_id:
                    path = link.path.partition('#')[0]
                    it   = self._session.query(Article).filter_by(domain_id = article.domain_id, path = path)
                    record['all'].update(it)
                    record['external'][domain] += it.count()
                    continue
                record['external'].update([link.domain.key])
            word_forms = {}
            # Phase 3: Key terms
            logging.info("Resolving key terms for %d in %s", doc_id, domain)
            for kwad in doc.keyword_adjacencies:
                word1, word2 = [x.lower() for x in [kwad.key1.word, kwad.key2.word]]
                if word1 in word_forms:
                    form = word_forms[word1]
                    form.append(word2)
                    word_forms.pop(word1, None)
                    word_forms[word2] = form
                else:
                    word_forms[word2] = [word1, word2]

            record['keywords'].update([' '.join(word_forms[w]) for w in word_forms])


        for domain in ret:
            logging.info("Computing overall statistics for %s", domain)
            src = ret[domain]

            # Compute coverage information 
            src['coverage'] = round(100.0*len(src['known'] - src['all'])/len(src['all'] | src['known']))
            src.pop('known', None)
            src.pop('all', None)

            # Compute a sample of keyterms
            src['keywords'] = [k for k,c in random.sample(src['keywords'].most_common(50), 15)]

            # Find out what gets linked to, 5 categories excluding 'other'
            new_summary = {}
            for dmkey, count in src['external'].most_common(5):
                new_summary[dmkey] = count 
            others = 0
            for dmkey in src['external']:
                if dmkey not in new_summary:
                    others += 1

            new_summary['others'] = 1
            src['external'] = new_summary

        return ret 

    def present(self, query_time):
        import json
        self.response['aux'] = self.additional()
        self.info['query_time'] = round(query_time, 2)
        return json.dumps(self.response, indent = 4)

class S3JSONResultPresenter(JSONResultPresenter):

    def __init__(self, keywords, query_text, engine):
        super(S3JSONResultPresenter, self).__init__(keywords, query_text, engine)
        self.query = self._session.query(UserQuery).filter_by(text = query_text).one()

    def present(self, query_time):
        response = super(S3JSONResultPresenter, self).present(query_time)
        connection = S3Connection()
        bucket = connection.get_bucket('results.sentimentron.co.uk')
        key = Key(bucket)
        key.key = 'results/'+str(self.query.id)
        key.set_contents_from_string(response)

        self.query.fulfilled = now()
        self._session.commit()

class QueryProcessor(object):

    def __init__(self, uq, engine, uq_session, presenter):

        self.domains    = set([])
        self.keywords   = set([])
        self.presenter  = presenter
        self.query_text = uq.text
        self._engine = engine

        self.uq      = uq 
        self._uq_session = uq_session

        self.kproc   = KQueryProcessor(self._engine)
        self.kdproc  = KDQueryProcessor(self._engine)
        self.fd      = FuzzyDomainResolutionService(self._engine)
        self.kwstack = MetaComboResolutionService([k(self._engine, None) for k in [FuzzyKeywordCaseResolutionService]])

    def execute(self):
        import time 

        start_time = time.time()
        for domain in self.uq.get_domains(self.query_text):
            self.domains.add(domain)
            self.domains.update(self.fd.resolve(domain))

        for keyword in self.uq.get_keywords(self.query_text):
            if keyword is None:
                continue
            self.keywords.add(keyword)
            self.keywords.update(self.kwstack.resolve(keyword))

        processor = None 
        if len(self.keywords) > 0 and len(self.domains) == 0:
            processor = self.kproc
        else:
            processor = self.kdproc 

        self.presenter = self.presenter(self.keywords, self.query_text, self._engine)
        dmset = set([])
        for row in processor.get_document_rows(self.keywords, self.domains, dmset):
            if type(row) is QueryMessage:
                self.uq.message = str(row)
                try:
                    self._uq_session.commit()
                except Exception as ex:
                    logging.error(("Unable to update query status!", row, uq))
            else:
                self.presenter.add_result(*row)
        self.presenter.present(time.time() - start_time)


if __name__ == "__main__":
    core.configure_logging('debug')
    engine = core.get_database_engine_string()
    logging.info("Using connection string '%s'" % (engine,))
    engine = create_engine(engine, encoding='utf-8', isolation_level = 'READ UNCOMMITTED', poolclass=SingletonThreadPool)

    kdproc  = KDQueryProcessor(engine)
    fd      = FuzzyDomainResolutionService(engine)
    kwstack = MetaComboResolutionService([k(engine, None) for k in [FuzzyKeywordCaseResolutionService]])
    session = Session(bind = engine)

    if "--cli" in sys.argv:
        query = raw_input("Enter query:")
        uq    = UserQuery(query)
        qp    = QueryProcessor(uq, engine, session, JSONResultPresenter)
        qp.execute()
    else:
        qq = QueryQueue(engine)
        for uq_id in qq:
            uq = session.query(UserQuery).get(uq_id)
            qp = QueryProcessor(uq, engine, session, S3JSONResultPresenter)
            pm = EmailProcessor()
            try:
                qp.execute()
                if uq.email is not None:
                    pm.send_success(uq.email, uq.id)
            except QueryException as ex:
                if uq.email is not None:
                    pm.send_failure(uq.email, ex.message)
                else:
                    uq.message = ex.message
                    uq.cancelled = True 
                    session.commit()
            qq.set_completed(uq_id)
