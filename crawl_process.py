#!/usr/bin/env python

# Crawl process helper

import sys

import core
from data import CrawlQueue, CrawlFileController
from data.db import CrawlController

if __name__ == "__main__":
	core.configure_logging()
	c = CrawlController(core.get_database_engine_string())
	q = CrawlQueue(c)
	r = CrawlFileController(c)

	for i in q:
		print list(r.read_CrawlFile(i))
		sys.exit(0)