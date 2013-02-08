#!/usr/bin/env python

import logging
import os

DB_PROT="mysql"
DB_NAME="sentimentron"

LOG_LEVELS = {'debug': logging.DEBUG,
	          'info': logging.INFO,
	          'warning': logging.WARNING,
	          'error': logging.ERROR,
	          'critical': logging.CRITICAL}


def check_environment():

	assert "SENT_DB_URL" in os.environ
	assert "SENT_DB_USER" in os.environ
	assert "SENT_DB_PASS" in os.environ
	assert "BOILERPIPE_URL" in os.environ

def get_database_engine_string():
	
	check_environment()
	host = os.environ["SENT_DB_URL"]
	user = os.environ["SENT_DB_USER"]
	pswd = os.environ["SENT_DB_PASS"]

	return "%s://%s:%s@%s/%s" % (DB_PROT, user, pswd, host, DB_NAME)

def configure_logging():

	if "SENT_PRODUCTION_LOG_LEVEL" not in os.environ:
		log_level = logging.DEBUG
	else:
		level = os.environ["SENT_PRODUCTION_LOG_LEVEL"]
		log_level = LOG_LEVELS[level]

	logging.basicConfig(level=log_level, format="%(asctime)-15s:%(filename)s:%(lineno)d: %(funcName)s, %(message)s")
