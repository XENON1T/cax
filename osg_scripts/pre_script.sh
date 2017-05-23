#!/bin/bash

# 1: run name
# 2: pax version
# 3: run number
# 4: logdir 
# 5: detector

#run="${1##*/}"

run=$1
pre_log=$4/pax_$2/$run/PRE_LOG
if [[ $5 == 'muon_veto' ]]; then
    pre_log=$4/pax_$2/${run}_MV/PRE_LOG
fi
echo $pre_log
echo "------ Start of prescript ------" >> $pre_log
date >> $pre_log
#echo "$pre_log" >> ~/pre_log_log
source activate pax_$2_OSG
source /home/ershockley/setup_rucio.sh

python /home/ershockley/cax/cax/dag_prescript.py $1 $2 $3 $5>> $pre_log 2>&1

ex=$?
echo "exiting with status $ex" >> $pre_log
exit $ex
