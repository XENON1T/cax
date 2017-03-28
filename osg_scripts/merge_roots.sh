#!/bin/bash

# 1: path to directory containing processed zips 

procdir=$1

run="${procdir##*/}" # crazy bash expression that extracts run name

echo $run 
cd $procdir

source deactivate
source activate mc
source /cvmfs/xenon.opensciencegrid.org/software/mc_old_setup.sh

hadd_mod -d -f ../$run.root XENON*root
#macro='/home/ershockley/cax/osg_scripts/polish.cc("'$run'")'

#echo $macro
#root -b -q "$macro"
if [[ ! $? -eq 0 ]]; then
    exit 1
fi

cd ..
rm -rf $procdir

source deactivate
