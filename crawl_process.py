#!/usr/bin/env python

# Crawl process helper

import multiprocessing
import logging
import os
import sys

import core
import requests

from backend import CrawlQueue, CrawlFileController, CrawlProcessor
from backend.db import CrawlController, SoftwareVersionsController, SoftwareVersion

from backend import pysen, pydate
from lxml import etree

def worker_init():
    global cp
    cp = CrawlProcessor(core.get_database_engine_string())

def worker_process(record):
    ret = cp.process_record(record)
    cp.finalize()
    return ret

if __name__ == "__main__":
    core.configure_logging()

    testing = "--testing" in sys.argv
    multi   = "--multi" in sys.argv

    c = CrawlController(core.get_database_engine_string())
    q = CrawlQueue(c)
    p = None
    sw = SoftwareVersionsController(core.get_database_engine_string())

    if multi:
        count = multiprocessing.cpu_count()*1.75
        count = int(count)
        logging.info("Running over %d processes...", count)
        p = multiprocessing.Pool(count, worker_init)
    else:
        worker_init()
    
    records = []
    r = CrawlFileController(c)

    if testing:
        for record in list(r.read_CrawlFileSQL('tmpSYh7nw', False)):
            if len(record) != 5:
                raise ValueError(record)
            headers, content, url, date_crawled, content_type = record
            headers, content, url, content_type = [str(i) for i in [headers, content, url, content_type]]
            records.append((None, (headers, content, url, date_crawled, content_type)))
        if multi:
            results = p.map(worker_process, records, 16)
        else:
            results = map(worker_process, records)
        print results
        sys.exit(0)
    else: # This is broken
        for crawl_file in q:
            record, records = r.read_CrawlFile(crawl_file), []
            if record is None:
                continue
            for rec in record:
                headers, content, url, date_crawled, content_type = rec
                headers, content, url, content_type = [str(i) for i in [headers, content, url, content_type]]
                records.append((crawl_file.id, (headers, content, url, date_crawled, content_type)))

            results = map(worker_process, records)
            print results
            r.mark_CrawlFile_complete(crawl_file)
            break
    

    results = None 
    if multi:
        results = p.map(worker_process, records, 16)
    else:
        results = map(worker_process, records)
    print results
    sys.exit(0)

    logging.debug("Creating CrawlProcessor...")
    cp = CrawlProcessor(core.get_database_engine_string())
    
    logging.debug("Processing crawl records...")
    success = [cp.process_record(record) for record in records]
    
    logging.debug("Comitting crawl records...")
    cp.finalize()

    #worker_init()
    #print map(worker_process, records)
    #worker_pool = multiprocessing.Pool(None, worker_init)
    #print worker_pool.map(worker_process, records)
