#!/bin/bash

# 1: raw directory
# 2: pax version
# 3: run number
# 4: logdir 

run="${1##*/}"
pre_log=$4/$2/$run/PRE_LOG
echo "------ Start of prescript ------" >> $pre_log
date >> $pre_log
#echo "$pre_log" >> ~/pre_log_log
source activate evan-testing
export PYTHONPATH=/home/ershockley/cax/lib/python3.4/site-packages/:$PYTHONPATH
cd /home/ershockley
python /home/ershockley/cax/setup.py install --prefix /home/ershockley/cax/
python /home/ershockley/cax/cax/dag_prescript.py $3 $2 >> $pre_log 2>&1
ex=$?
echo "exiting with status $ex" >> $pre_log
exit $ex