#!/usr/bin/env python

# Crawl Processor 
import itertools
import logging
import os
import sys
import threading
import types

from collections import Counter

from sqlalchemy import create_engine
from sqlalchemy.exc import *
from sqlalchemy.orm.session import Session 
from topia.termextract import extract
from nltk.tokenize import sent_tokenize
import nltk
from lxml import etree
from bs4 import BeautifulSoup
from pysen.documents import DocumentClassifier

import langid
import pydate
import pysen
import pysen.models
import MySQLdb as mdb


from db import Article, Domain, DomainController, ArticleController
from db import Keyword, KeywordController
from db import SoftwareVersionsController
from db import Document, Sentence, Phrase
from db import KeywordIncidence, SoftwareInvolvementRecord
from db import CertainDate, AmbiguousDate, KeywordAdjacency
from db import RelativeLink, AbsoluteLink

KEYWORD_LIMIT = 32

class KeywordSet(object):

    def __init__(self, stop_list):
        self.keywords = set([])
        self.stop_list = stop_list
        self._cache = {}

    def __len__(self):
        return len(self.keywords)

    def __str__(self):
        return "KeywordSet(%s)" % (str(self.keywords),)

    def __iter__(self):
        for item in self.keywords:
            yield item

    def add(self, term):
        
        if len(self) > KEYWORD_LIMIT:
            raise ValueError("KEYWORD_LIMIT exceeded.")

        term = term.strip()

        if not term.lower() in self.stop_list:
            self.keywords.add(term)
            return True 

        return False 

    def convert_adj_tuples(self, tuple_list, keyword_map, kwc):

        ret = []

        for i, j in tuple_list:
            try:
                i, j = i.strip(), j.strip()
                i = kwc.get_Keyword_fromId(keyword_map[i])
                j = kwc.get_Keyword_fromId(keyword_map[j])
                if i is not None and j is not None:
                    ret.append((i,j))
                else:
                    logging.error("Couldn't resolve keyword pair.")
            except ValueError as ex:
                logging.error(ex)
            except KeyError as ex:
                logging.info(ex)
        return ret


    def convert(self, keyword_map, kwc):
        ret = []
        short = []
        for t in self.keywords:
            try:
                ident = keyword_map[t]
                k = kwc.get_Keyword_fromId(ident)
                if k is not None:
                    ret.append(k)
                    continue
                logging.error("Can't resolve keyword %s with iden %d", t, ident)
            except ValueError as ex:
                logging.error(ex)
            except KeyError as ex:
                logging.info(ex)

        return ret, short

class CrawlProcessor(object):

    __VERSION__ = "CrawlProcessor-0.1"

    def __init__(self, engine, stop_list="keyword_filter.txt"):

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
        self._session = Session(bind=self._engine, autocommit = False, autoflush = True)

        if type(stop_list) == types.StringType:
            stop_list_fp = open(stop_list)
        else:
            stop_list_fp = stop_list 

        self.stop_list = set([])
        for line in stop_list_fp:
            self.stop_list.add(line.strip())

        self.cls = DocumentClassifier()
        self.dc  = DomainController(self._engine, self._session)
        self.ac  = ArticleController(self._engine, self._session)
        self.ex  = extract.TermExtractor()
        self.kwc = KeywordController(self._engine, self._session)
        self.swc = SoftwareVersionsController(self._engine, self._session)
        self.drw = DomainResolutionWorker()

    def process_record(self, item):
        try:
            if len(item) != 2:
                raise ValueError(item)
            self._process_record(item)
        except Exception as ex:
            import traceback
            print >> sys.stderr, ex
            traceback.print_exc()
            raise ex 

    def _process_record(self, item_arg):

        crawl_id, record = item_arg
        headers, content, url, date_crawled, content_type = record

        assert headers is not None
        assert content is not None 
        assert url is not None 
        assert date_crawled is not None 
        assert content_type is not None 

        status = "Processed"

        # Sort out the domain
        domain_identifier = None 
        logging.info("Retrieving domain...")
        domain_key = self.dc.get_Domain_key(url)
        while domain_identifier == None:
            domain_identifier = self.drw.get_domain(domain_key)

        domain = self._session.query(Domain).get(domain_identifier)
        assert domain is not None

        # Build database objects 
        path   = self.ac.get_path_fromurl(url)
        article = Article(path, date_crawled, crawl_id, domain, status)
        classified_by = self.swc.get_SoftwareVersion_fromstr(pysen.__VERSION__)
        assert classified_by is not None

        if content_type != 'text/html':
            logging.error("Unsupported content type: %s", str(content_type))
            article.status = "UnsupportedType"
            return False

        # Start the async transaction to get the plain text
        worker_req_thread = BoilerPipeWorker(content)
        worker_req_thread.start()

        # Whilst that's executing, parse the document 
        logging.info("Parsing HTML...")
        html = BeautifulSoup(content)

        # Extract the dates 
        date_dict = pydate.get_dates(html)

        if len(date_dict) == 0:
            status = "NoDates"

        # Detect the language
        lang, lang_certainty = langid.classify(content)

        # Wait for the BoilerPipe thread to complete
        worker_req_thread.join()
        logging.debug(worker_req_thread.result)
        logging.debug(worker_req_thread.version)

        if worker_req_thread.result == None:
            article.status = "NoContent"
            return False

        # If the language isn't English, skip it
        if lang != "en":
            logging.info("language: %s with certainty %.2f - skipping...", lang, lang_certainty)
            article.status = "LanguageError" # Replace with something appropriate
            return False

        content = worker_req_thread.result.encode('ascii', 'ignore')

        # Headline extraction 
        h_counter = 6
        headline = None
        while h_counter > 0:
            tag = "h%d" % (h_counter,)
            found = False 
            for node in html.findAll(tag):
                if node.text in content:
                    headline = node.text 
                    found = True 
                    break 
            if found:
                break
            h_counter -= 1

        # Run keyword extraction 
        keywords = self.ex(content)
        kset     = KeywordSet(self.stop_list)
        nnp_sets_scored = set([])

        for word, freq, amnt in sorted(keywords):
            try:
                nnp_sets_scored.add((word, freq))
            except ValueError:
                break 

        nnp_adj = set([])
        nnp_set = set([])
        nnp_vector = []
        for sentence in sent_tokenize(content):
            text = nltk.word_tokenize(sentence)
            pos  = nltk.pos_tag(text)
            pos_groups = itertools.groupby(pos, lambda x: x[1])
            for k, g in pos_groups:
                if k != 'NNP':
                    continue
                nnp_list = [word for word, speech in g]
                nnp_buf = []
                for item in nnp_list:
                    nnp_set.add(item)
                    nnp_buf.append(item)
                    nnp_vector.append(item)
                for i, j in zip(nnp_buf[0:-1], nnp_buf[1:]):
                    nnp_adj.add((i, j))

        nnp_counter = Counter(nnp_vector)
        for word in nnp_set:
            score = nnp_counter[word]
            nnp_sets_scored.add((item, score))

        for item, score in sorted(nnp_sets_scored, key=lambda x: x[1], reverse=True):
            try: 
                if type(item) == types.ListType or type(item) == types.TupleType:
                    kset.add(' '.join(item))
                else:
                    kset.add(item)
            except ValueError:
                break 

        # Generate list of all keywords
        keywords = set([])
        for keyword in kset:
            try:
                k = Keyword(keyword)
                keywords.add(k)
            except ValueError as ex:
                logging.error(ex)
                continue
        for item1, item2 in nnp_adj:
            try:
                k = Keyword(item1)
                keywords.add(k)
            except ValueError as ex:
                logging.error(ex)
            try:
                k = Keyword(item2)
                keywords.add(k)
            except ValueError as ex:
                logging.error(ex)

        # Resolve keyword identifiers
        keyword_resolution_worker = KeywordResolutionWorker([k.word for k in keywords])
        keyword_resolution_worker.start()
            

        # Run sentiment analysis
        trace = []
        features = self.cls.classify(worker_req_thread.result, trace) 
        label, length, classified, pos_sentences, neg_sentences,\
        pos_phrases, neg_phrases  = features[0:7]        

        # Convert Pysen's model into database models
        try:
            doc = Document(article, label, length, pos_sentences, neg_sentences, pos_phrases, neg_phrases, headline)
        except ValueError as ex:
            logging.error(ex)
            logging.error("Skipping this document...")
            article.status = "ClassificationError"
            return False

        self._session.add(doc)
        extracted_phrases = set([])
        for sentence, score, phrase_trace in trace:
            sentence_type = "Unknown"
            for node in html.findAll(text=True):
                if sentence.text in node.strip():
                    sentence_type = node.parent.name.upper()
                    break

            if sentence_type not in ["H1", "H2", "H3", "H4", "H5", "H6", "P", "Unknown"]:
                sentence_type = "Other"

            label, average, prob, pos, neg, probs, _scores = score 

            s = Sentence(doc, label, average, prob, sentence_type)
            self._session.add(s)
            for phrase, prob, score, label in phrase_trace:
                p = Phrase(s, score, prob, label)
                self._session.add(p)
                extracted_phrases.add((phrase, p))

        # Wait for keyword resolution to finish
        keyword_resolution_worker.join()
        keyword_mapping = keyword_resolution_worker.out_keywords

        # Associate extracted keywords with phrases
        keyword_objects, short_keywords = kset.convert(keyword_mapping, self.kwc)
        for k in keyword_objects:
            self._session.merge(k)
        for p, p_obj in extracted_phrases:
            for k in keyword_objects:
                if k.word in p.get_text():
                    nk = KeywordIncidence(k, p_obj)

        # Save the keyword adjacency list
        for i, j in kset.convert_adj_tuples(nnp_adj, keyword_mapping, self.kwc):
            self._session.merge(i)
            self._session.merge(j)
            kwa = KeywordAdjacency(i, j, doc)
            self._session.add(kwa)

        # Build date objects
        for key in date_dict:
            rec  = date_dict[key]
            if "dates" not in rec:
                logging.error("OK: 'dates' is not in a pydate result record.")
                continue
            dlen = len(rec["dates"])
            if dlen > 1:
                for date, day_first, year_first in rec["dates"]:
                    dobj = AmbiguousDate(date, doc, day_first, year_first, rec["prep"])
                    self._session.add(dobj)
            elif dlen == 1:
                for date, day_first, year_first in rec["dates"]:
                    dobj = CertainDate(date, doc)
                    self._session.add(dobj)
            else:
                logging.error("'dates' in a pydate result set contains no records.")

        # Process links
        for link in html.findAll('a'):
            if not link.has_attr("href"):
                logging.debug("skipping %s: no href", link)
                continue

            process = True 
            for node in link.findAll(text=True):
                if node not in worker_req_thread.result:
                    process = False 
                    break 
            
            if not process:
                logging.debug("skipping %s because it's not in the body text", link)
                break

            href, junk, junk = link["href"].partition("#")
            if "http://" in href:
                try:
                    href_domain = self.dc.get_Domain_fromurl(href)
                except ValueError as ex:
                    logging.error(ex)
                    logging.error("Skipping this link")
                    continue
                href_path   = self.ac.get_path_fromurl(href)
                lnk = AbsoluteLink(doc, href_domain, href_path)
                self._session.add(lnk)
                logging.debug("Adding: %s", lnk)
            else:
                href_path  = href 
                try:
                    lnk = RelativeLink(doc, href_path)
                except ValueError as ex:
                    logging.error(ex)
                    logging.error("Skipping link")
                    continue
                self._session.add(lnk)
                logging.debug("Adding: %s", lnk)

        # Construct software involvment records
        self_sir = SoftwareInvolvementRecord(self.swc.get_SoftwareVersion_fromstr(self.__VERSION__), "Processed", doc)
        date_sir = SoftwareInvolvementRecord(self.swc.get_SoftwareVersion_fromstr(pydate.__VERSION__), "Dated", doc)
        clas_sir = SoftwareInvolvementRecord(self.swc.get_SoftwareVersion_fromstr(pysen.__VERSION__), "Classified", doc)
        extr_sir = SoftwareInvolvementRecord(self.swc.get_SoftwareVersion_fromstr(worker_req_thread.version), "Extracted", doc)

        for sw in [self_sir, date_sir, clas_sir, extr_sir]:
            self._session.merge(sw, load=True)

        logging.debug("Domain: %s", domain)
        logging.debug("Path: %s", path)
        article.status = status

        # Commit to database, return True on success
        self._session.commit()
        return True

    def finalize(self):
        self._session.commit()

class DomainResolutionWorker(object):

    def __init__(self):
        import MySQLdb as mdb
        logging.info("Domain resolution: opening DB...")
        host = os.environ["SENT_DB_URL"]
        user = os.environ["SENT_DB_USER"]
        pswd = os.environ["SENT_DB_PASS"]
        self.con = mdb.connect(host, user, pswd, "sentimentron")

    def get_domain(self, domain):

        cur = self.con.cursor()

        # Check if the domain exists
        logging.info("DomainResolutionWorker: checking %s..." % domain)
        sql = "SELECT id FROM domains WHERE `key` = %s"
        cur.execute(sql,(domain,))
        for row, in cur:
            logging.info((domain, row))
            return row 

        # If it doesn't, create it
        sql = "INSERT IGNORE INTO domains (`key`,`date`) VALUES (%s, NOW())"
        logging.debug(sql, domain)
        logging.info("DomainResolutionWorker: inserting %s..." % domain)
        try:
            cur.execute(sql,(domain,))
            self.con.commit()
        except mdb.OperationalError as ex:
            return None 

class KeywordResolutionWorker(threading.Thread):

    def __init__(self, keywords):
        self.in_keywords  = keywords
        self.out_keywords = {}
        threading.Thread.__init__(self)


    def run(self):
        # Open the mysqldatabase connection
        logging.info("Keyword resolution: opening DB...")
        host = os.environ["SENT_DB_URL"]
        user = os.environ["SENT_DB_USER"]
        pswd = os.environ["SENT_DB_PASS"]
        con = mdb.connect(host, user, pswd, "sentimentron")

        # Create the 1st-phase query
        sql = "INSERT IGNORE INTO keywords (`word`) VALUES (%s)"

        cur = con.cursor()
        logging.info("Keyword resolution: 1st-phase INSERT")
        #logging.debug(self.in_keywords)
        cur.executemany(sql, [(k,) for k in self.in_keywords])
        con.commit()

        # Now the 2nd-phase, getting the IDs
        sql = "SELECT id FROM keywords WHERE `word` = %s"
        for key in self.in_keywords:
            cur.execute(sql, (key,))
            identifier, = cur.fetchone()
            thing = (key, identifier)
            logging.debug(thing)
            self.out_keywords[key] = identifier

class BoilerPipeWorker(threading.Thread):

    def __init__(self, body_text):
        self.orig = body_text 
        self.result = None 
        self.version = None
        threading.Thread.__init__(self)

    def run(self):
        import requests
        post = {"charset": "UTF-8", "content": self.orig, "method":"default"}
        r = requests.post(os.environ["BOILERPIPE_URL"], data = post)
        r.raise_for_status()

        try:
            parsed  = etree.fromstring(r.text)
        except Exception as ex:
            logging.critical("Failed to parse: %s (%s)", r.text, ex)
            return 

        server_node = parsed.find("ServerInfo")
        if server_node is None:
            raise ValueError("No ServerInfo")

        self.version = "Unknown"
        version = server_node.find("Version")
        if version is None:
            raise ValueError("Couldn't find version information: %s" % (r.text,))
        self.version = version.text

        logging.debug(parsed)
        if parsed.find("ExtractionFailureResponse") is not None:
            return None

        content = parsed.find("Response")
        if content is None:
            raise ValueError(("Couldn't find response", r.text))

        self.result = content.text 