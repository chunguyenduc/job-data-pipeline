#!/bin/bash

set -x
BASEDIR=${PWD}
MODULE_LIST=find . -type f \( -iname "*.py" ! -iname "*__init__.py" ! -iname "*test_*.py" ! -iname "*config*.py" \)
echo $BASEDIR
cd $BASEDIR/dags
ls -l
coverage run -m unittest discover
coverage report $(MODULE_LIST)