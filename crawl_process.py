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

	for i in q:
		print list(r.read_CrawlFile(i))
		sys.exit(0)