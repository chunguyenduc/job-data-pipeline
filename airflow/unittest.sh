#!/bin/bash

set -x
BASEDIR=${PWD}
echo $BASEDIR
cd $BASEDIR/dags
MODULE_LIST=`find . -type f \( -iname "*.py" ! -iname "*__init__.py" ! -iname "*test_*.py" ! -iname "*config*.py" \)`
ls -l
chmod -R 777 .
coverage run -m unittest discover
coverage report $MODULE_LIST