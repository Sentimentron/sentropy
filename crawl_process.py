#!/usr/bin/env python

# Crawl process helper

import core
from data import CrawlQueue
from data.db import CrawlController

if __name__ == "__main__":
	core.configure_logging()
	c = CrawlController(core.get_database_engine_string())
	q = CrawlQueue(c)

	for i in q:
		print i.id