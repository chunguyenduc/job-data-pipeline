#!/bin/bash

su -l airflow
set -x
BASEDIR=${PWD}
cd $BASEDIR/dags
ls -l
python3 -m unittest discover -v
python3 -m coverage run -m unittest
