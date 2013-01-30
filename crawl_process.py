#!/usr/bin/env python

# Crawl process helper

import multiprocessing
import sys

import core
from backend import CrawlQueue, CrawlFileController, CrawlProcessor
from backend.db import CrawlController

def worker_init():
	global cp 
	cp = CrawlProcessor()

def worker_process(record):
	return cp.process_record(record)

if __name__ == "__main__":
	core.configure_logging()
	c = CrawlController(core.get_database_engine_string())
	q = CrawlQueue(c)
	r = CrawlFileController(c)

	p = multiprocessing.Pool()

	records = []

	for i in q:
		record = r.read_CrawlFile(i)
		for rec in record:
			records.append(rec)
			break
	
	worker_pool = Pool(None, worker_init)

	print worker_pool.map(worker_process, records)
