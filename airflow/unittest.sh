#!/bin/bash


BASEDIR=${PWD}
cd $BASEDIR/airflow/dags
python3 -m unittest discover -v