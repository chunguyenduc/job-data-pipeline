#!/bin/bash


BASEDIR=${PWD}
echo $BASEDIR
cd $BASEDIR/dags
ls -l
python3 -m unittest discover -v
coverage run -m unittest discover && coverage report