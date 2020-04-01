#!/bin/bash

# 1: run name
# 2: pax version
# 3: run number
# 4: general log directory
# 5: number of zips
# 6: detector

# make tmp directory and go there
workdir=$(mktemp -d)
cd $workdir
run="${1##*/}"
post_log=$4/pax_$2/$run/POST_LOG
if [[ $6 == 'muon_veto' ]]; then
    post_log=$4/pax_$2/${run}_MV/POST_LOG  
fi
# remove previous POST logs
if [[ -e ${post_log} ]]
then
    rm ${post_log}
fi

echo "------ Start of post script ---------" >> $post_log
date >> $post_log

echo $@ >> $post_log

if [[ $6 == "muon_veto" ]]
then
    rootfile=/xenon/xenon1t_processed/pax_$2/$1_MV.root
else
    rootfile=/xenon/xenon1t_processed/pax_$2/$1.root
fi

source activate pax_$2
#source $HOME/cax/osg_scripts/setup_rucio.sh


cax_dir=$HOME/cax

#if [[ ! -e $rootfile ]]; then
rawdir=$1

# save exit status of pipe
set -o pipefail


# move to scratch space. This way we don't have to rewrite the pax_event_classes for each job
scratchdir="/scratch/ershockley/pax_$2"
mkdir -p $scratchdir
cd $scratchdir

python ${cax_dir}/osg_scripts/upload.py $1 $5 $2 $6 >> $post_log 2>&1

if [[ $? -ne 0 ]]; then
    echo "hadd or DB stuff failed, exiting" >> $post_log
    exit 1
fi	  


# copy to midway
source /cvmfs/xenon.opensciencegrid.org/releases/nT/development/setup.sh

export X509_USER_PROXY=/xenon/grid_proxy/xenon_service_proxy

gfal-copy -p -f file:///${rootfile} gsiftp://sdm06.rcc.uchicago.edu:2811/dali/lgrandi/xenon1t/processed/pax_$2/$1.root 2>&1

