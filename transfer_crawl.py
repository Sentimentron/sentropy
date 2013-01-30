#!/usr/bin/env python

#
# Transfers the contents of a SQLite database file containing crawl keys
# into the main crawl database. Crawl keys are marked as unprocessed.
#

import logging
import sys
import sqlite3

import core
from backend.db import CrawlSource, CrawlFile, CrawlController

def _transfer(db_file, key_prefix, key_type, safety=False):
	if key_type not in ["ARFF", "SQL", "Text"]:
		raise ValueError(("Not valid", src_key_type))

	print >> sys.stderr, "Just to confirm:"
	print >> sys.stderr, "\tdb_file:", db_file
	print >> sys.stderr, "\tkey_prefix:", key_prefix
	print >> sys.stderr, "\tkey_type:", key_type 
	print >> sys.stderr, "\tsafety:", safety

	resp = raw_input()
	if resp != 'y':
		return

	controller = CrawlController(core.get_database_engine_string())

	# Create the crawl source first, if it doesn't already exist
	src = controller.get_CrawlSource(key_prefix)
	if src is None:
		logging.info("Creating crawl source...")
		src = CrawlSource(key_prefix)
		controller.attach_CrawlSource(src)
		controller.commit()

	# Get each of the records, insert as necessary
	con = sqlite3.connect(db_file)
	sql = "SELECT key FROM keys"

	cur = con.cursor()
	cur.execute(sql)

	inserted = 0

	for row in cur:
		key, = row
		rec = CrawlFile(key, src, key_type)
		controller.attach_CrawlFile(rec)
		inserted += 1
		if safety or (inserted % 2000) == 0:
			controller.commit()

	controller.commit()
	controller.deduplicate()

if __name__ == "__main__":

	core.configure_logging()

	src_file = None
	src_key_type = None 
	src_bucket_prefix = None 
	
	safe_commit = "--safe" in sys.argv
	for pos, arg in enumerate(sys.argv):
		if arg == "--type":
			src_key_type = sys.argv[pos+1]
		elif arg == "--prefix":
			src_key_prefix = sys.argv[pos+1]
		elif arg == "--file":
			src_file = sys.argv[pos+1]
		elif arg == "--help":
			print "usage: python transfer_crawl.py --type [ARFF|SQL|Text]", \
			"--prefix [bucket prefix] --file [input_file]"
			sys.exit(0)

	if src_key_type not in ["ARFF", "SQL", "Text"]:
		raise ValueError(("Not valid", src_key_type))

	_transfer(src_file, src_key_prefix, src_key_type, safe_commit)
