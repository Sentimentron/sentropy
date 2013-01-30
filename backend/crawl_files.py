#!/usr/bin/env python

import cStringIO
import logging
import os
import subprocess
import tempfile
import sqlite3

import boto
from boto.s3.bucket         import Bucket
from boto.s3.key          	import Key

from db import CrawlFile, CrawlController

class CrawlFileController(object):

	def __init__(self, controller):
		self._controller = controller

	def download_CrawlFile(self, which):
		logging.info("Connecting to S3...")
		conn   = boto.connect_s3()
		bucket = which.src.key 

		logging.info("Downloading %s from bucket %s", which.key, bucket)
		bucket = Bucket(connection=conn, name=bucket)
		key    = Key(bucket)
		key.key = which.key 

		if not key.exists():
			logging.info("Key %s doesn't exist in %s, marking as Error", which.key, bucket)
			which.status = "Error"

		#tmp = tempfile.mktemp(suffix='bz2.sql', prefix='db-')
		#fp  = open(tmp, 'wb')
		fp = tempfile.TemporaryFile()
		logging.info("Downloading %s...", which.key)
		key.get_contents_to_file(fp)
		fp.seek(0)
		logging.info("Completed downloading %s...", which.key)
		return fp

	def read_CrawlFileSQL(self, fp):
		_junk, fname = tempfile.mkstemp()
		logging.info("Decompressing to %s...", fname)
		decompressed_fp = open(fname, 'w+b')
		subprocess.check_call(["xz", "-d"], stdin=fp, stdout=decompressed_fp)
		decompressed_fp.close()

		logging.info("Opening database...")
		db = sqlite3.connect(fname)
		cur = db.cursor()
		cur.execute("SELECT headers, content, site, date_crawled, content_type FROM articles")
		for row in cur:
			headers, content, site, date_crawled, content_type = row 
			if content_type != 'text/html':
				logging.error("Unsupported content type: %s", str(row))
				continue 
			yield (headers, content, site, date_crawled, content_type)

		db.close()
		logging.info("Deleting %s...", fname)
		os.remove(fname)


	def read_CrawlFile(self, which):

		fp = self.download_CrawlFile(which)

		if which.kind == "SQL":
			return self.read_CrawlFileSQL(fp)
		else:
			raise Exception("Unimplemented")

