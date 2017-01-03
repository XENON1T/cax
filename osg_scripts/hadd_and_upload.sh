#!/bin/bash

# 1: raw directory
# 2: pax version
# 3: run number
# 4: general log directory

run="${1##*/}"
post_log=$4/$2/$run/POST_LOG
echo "------ Start of post script ---------" >> $post_log
date >> $post_log

echo $@ >> $post_log

rawdir=$1

source activate evan-testing 

cd /home/ershockley/cax/
export PYTHONPATH=/home/ershockley/cax/lib/python3.4/site-packages/:$PYTHONPATH
python setup.py install --prefix ${PWD}

python /home/ershockley/cax/osg_scripts/upload.py $1 $2 >> $post_log 2>&1

if [[ $? -ne 0 ]]; then
    exit 1
fi
    
# transfer to midway
echo "Beginning cax transfer to midway" >> $post_log
/home/ershockley/cax/bin/cax --once --run $3 --config /home/ershockley/cax/cax/cax_transfer.json >> $post_log 2>&1

ex=$?

echo "exiting with status $ex" >> $post_log
echo

exit $ex