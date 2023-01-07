#!/bin/bash

set -x
BASEDIR=${PWD}
echo $BASEDIR
cd $BASEDIR/dags
MODULE_LIST=`find . -type f \( -iname "*.py" ! -iname "*__init__.py" ! -iname "*test_*.py" ! -iname "*config*.py" \)`
ls -l
coverage run --data-file tests/.coverage -m unittest discover
coverage report --data-file tests/.coverage $MODULE_LIST