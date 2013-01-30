#!/usr/bin/env python

# Crawl process helper

import multiprocessing
import sys

import core
from backend import CrawlQueue, CrawlFileController, CrawlProcessor
from backend.db import CrawlController

def worker_init():
	global cp 
	cp = CrawlProcessor(core.get_database_engine_string())

def worker_process(record):
	return cp.process_record(record)

if __name__ == "__main__":
	core.configure_logging()

	testing = "--testing" in sys.argv

	c = CrawlController(core.get_database_engine_string())
	q = CrawlQueue(c)
	r = CrawlFileController(c)
	p = multiprocessing.Pool()

	records = []

	if testing:
		for record in list(r.read_CrawlFileSQL('tmpSYh7nw', False))[3:7]:
			records.append((None, record))
	else:
		for i in q:
			record = r.read_CrawlFile(i)
			if record is None:
				continue
			for rec in record:
				records.append((i.crawl_id, rec))
				break
	
	worker_init()
	print map(worker_process, records)
	#worker_pool = multiprocessing.Pool(None, worker_init)
	#print worker_pool.map(worker_process, records)
