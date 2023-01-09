#!/bin/bash

set -x
BASEDIR=${PWD}
cd $BASEDIR/dags
ls -l
python3 -m unittest discover -v