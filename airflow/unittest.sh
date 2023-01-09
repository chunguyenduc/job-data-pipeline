#!/bin/bash

set -x
BASEDIR=${PWD}
MODULE_LIST=`find . -type f \( -iname "*.py" ! -iname "*__init__.py" ! -iname "*test_*.py" ! -iname "*config*.py" \)`
cd $BASEDIR/dags
ls -l -a
python3 -m unittest discover -v
