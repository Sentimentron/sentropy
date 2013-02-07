#!/usr/bin/env python

import logging
import itertools
import types
import core

from backend.db import *
from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 

class Query(object):

    def __init__(self, keywords, domains, engine):

        if type(engine) == types.StringType:
            logging.info("Using connection string '%s'" % (engine,))
            new_engine = create_engine(engine, encoding='utf-8', isolation_level="READ COMMITTED")
            if "sqlite:" in engine:
                logging.debug("Setting text factory for unicode compat.")
                new_engine.raw_connection().connection.text_factory = str 
            self._engine = new_engine
        else:
            logging.info("Using existing engine...")
            self._engine = engine
        logging.info("Binding session...")
        self._session = Session(bind=self._engine, autocommit = False)

        self.keywords = keywords
        self.domains  = domains

        self.unresolved_domains = set([])
        self.unresolved_keywords = set([])

        self.resolved_domains = set([])
        self.resolved_keywords = set([])

        self.keyword_adjacencies_docs = set([])

    @classmethod
    def get_keywords(cls, q):
        chunks = q.split(' ')
        ret = []
        for c in chunks:
            valid = False
            for l in c:
                valid = valid or (l >= 'a' and l <= 'z')
                valid = valid or (l >= 'A' and l <= 'Z')
                valid = valid or (l >= '0' and l <= '9')
            if not valid:
                continue
            ret.append(c)
        return ret 

    @classmethod
    def get_domains(cls, q):
        chunks = q.split(' ')
        return filter(lambda x: '.' in x, chunks)

    def resolve_domains(self):
        domain_objs = set([])
        session = self._session
        for domain in self.domains:
            like_str = '%%%s' % (domain,)
            it = session.query(Domain).filter(key.like(like_str))
            if it.count() == 0:
                uresolved_domains.add(domain)
                continue
            for d in it:
                domain_objs.append(d)
        self.resolved_domains = domain_objs

    def resolve_keywords(self):
        keywords = []
        session = self._session
        for keyword in self.keywords:
            it = set(session.query(Keyword).filter(Keyword.word.like(keyword)).all())
            if len(it) == 0:
                self.uresolved_keywords.add(keyword)
                continue
            keywords.append(it)
        self.resolved_keywords = keywords

    def gen_keyword_pairs(self):
        return itertools.combinations(self.keywords, 2)

    def resolve_keyword_adjacencies(self):
        session = self._session
        conn    = self._engine.connect()
        adj = set([])
        ret = set([])

        for key1, key2 in self.gen_keyword_pairs():
            sql = ("""SELECT doc_id, word1, word2 FROM (SELECT doc_id, key1_id, key2_id, word1, keywords.word AS word2 FROM (
                SELECT doc_id, key1_id, key2_id, word AS word1 FROM keyword_adjacencies JOIN keywords on key1_id = keywords.id
                ) k JOIN keywords ON k.key2_id = keywords.id) sea WHERE word1 LIKE '%s' AND `word2` LIKE '%s'""" % (key1, key2))
            logging.debug(sql)
            result = conn.execute(sql)
            for row in result:
                adj.add((row["doc_id"], row["word1"], row["word2"]))

        word_set = map(lambda x: x.word, itertools.chain.from_iterable(self.resolved_keywords))

        logging.debug(word_set)
        for doc_id, word1, word2 in adj:
            if word1 in word_set and word2 in word_set:
                ret.add(doc_id)

        self.keyword_adjacencies_docs = ret

    def resolve_keyword_incidences(self):
        pass

if __name__ == "__main__":

    core.configure_logging()
    q = Query(Query.get_keywords("Apple Store"), [], core.get_database_engine_string())
    q.resolve_keywords()
    q.resolve_keyword_adjacencies()
    raw_input(q.keyword_adjacencies_docs)