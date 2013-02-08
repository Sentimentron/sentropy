#!/usr/bin/env

#
# Crawl Queue Controller
#

import logging
import types

import boto.sqs 
from db import CrawlController, CrawlFile, CrawlSource
from boto.sqs.message import Message

DEFAULT_QUEUE_NAME = "process-queue"
SQS_REGION = "us-east-1"

class ProcessQueue(object):

	def __init__(self, controller, queue_name=None):

		if not isinstance(controller, CrawlController):
			raise TypeError(type(controller))

		self._controller = controller
		self._messages   = {}

		if queue_name is None:
			queue_name = DEFAULT_QUEUE_NAME
		self._queue_name = queue_name

		logging.info("Using '%s' as the queue.", (queue_name,))

		self._conn  = boto.sqs.connect_to_region(SQS_REGION)
		self._queue = self._conn.lookup(queue_name)
		if self._queue is None:
			logging.info("Creating '%s'...", (queue_name,))
			self._queue = self._conn.create_queue(queue_name, 120)

		logging.info("Connection established.")

	def __iter__(self):

		while 1:
			if not self._get_queueItemAvailabilityStatus():
				if not self._replenish_queue():
					break

			rs = self._queue.get_messages()
			logging.info("Currently %d items in the queue", len(rs))
			for item in rs:
				iden = int(item.get_body())
				self._messages[iden] = item
				y = self._controller.get_CrawlFile_fromid(iden)
				if y.status != "Incomplete":
					continue
				yield y

	def set_completed(self, what):
		if type(what) == CrawlFile:
			what = what.id 

		if type(what) is not types.IntType and type(what) is not types.LongType:
			raise TypeError(type(what))

		logging.info("Marking %d as completed...", what)

		msg = self._messages[what]
		self._queue.delete_message(msg)
		del msg

	def _get_queueItemAvailabilityStatus(self):
		status = self._queue.count() > 0
		if not status:
			logging.info("%s is under the item limit", self._queue_name)
		return status