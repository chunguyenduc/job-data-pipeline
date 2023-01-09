#!/bin/bash

set -x
BASEDIR=${PWD}
MODULE_LIST=`find . -type f \( -iname "*.py" ! -iname "*__init__.py" ! -iname "*test_*.py" ! -iname "*config*.py" \)`
cd $BASEDIR/dags
ls -l
python3 -m unittest discover -v
python3 -m coverage run -m unittest
ls -l
chmod u+x /opt/airflow/dags/.coverage
python3 -m coverage report $(MODULE_LIST)
