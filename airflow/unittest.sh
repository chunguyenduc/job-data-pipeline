#!/bin/bash


BASEDIR=${PWD}
echo BASEDIR
ls -l
cd $BASEDIR/dags
python3 -m unittest discover -v