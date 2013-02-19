#!/bin/bash 

CMD="python cli_query_submit.py"

$CMD Obama > ~/site/samples/obama.json
$CMD foxnews.com > ~/site/samples/foxnews.json
$CMD foxnews.com Democrat > ~/site/samples/foxnews_democrat.json
$CMD foxnews.com Republican > ~/site/samples/foxnews_republican.json
$CMD guardian.co.uk > ~/site/samples/guardian.json
$CMD Apple Computer > ~/site/samples/apple_computer.json
