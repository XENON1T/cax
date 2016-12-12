#!/bin/bash

# 1: raw directory
# 2: pax version
# 3: Job exit code
# 4: run number
# 5: general log directory

run="${1##*/}"
post_log=$5/$2/$run/POST_LOG
echo "------ Start of post script ---------" >> $post_log
date >> $post_log

echo $@ >> $post_log

if [[ $3 -eq "-1004" ]]; then
    echo "Job did not run. Skipping post script" >> $post_log
    echo "Exiting with status 0" >> $post_log
    exit 0
fi

rawdir=$1

python /home/ershockley/cax/osg_scripts/upload.py $1 $2 >> $post_log 2>&1

if [[ $? -ne 0 ]]; then
    exit 1
fi
    
# transfer to midway
echo "Beginning cax transfer to midway" >> $post_log
/home/ershockley/cax/bin/cax --once --run $4 --config /home/ershockley/cax/cax/cax_transfer.json >> $post_log 2>&1

ex=$?

echo "exiting with status $ex" >> $post_log
echo

exit $ex