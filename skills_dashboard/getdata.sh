#!/bin/bash
exec 3>&1 4>&2
trap 'exec 2>&4 1>&3' 0 1 2 3
exec 1>log.out 2>&1
cd skillscraper/skillscraper
# echo 'what the fuck'
python3 crawl.py
cd ..