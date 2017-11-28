from cax.dag_writer import dag_writer
import numpy as np
from make_runlist import make_runlist
from pax import __version__
import os

runlist = []
with open('/home/ershockley/fei_list_num.py') as f:
    for line in f.readlines():
        runlist.append(int(line.rstrip()))

config = { 'runlist' : runlist,
           'pax_version' :'v' +  __version__,
           'logdir' : '/scratch/processing',
           'retries' : 9,
           'specify_sites' : [],
 	   "exclude_sites": ["Comet"], 
           'host' : 'login',
           'use_midway' : False, # this overrides the specify and exclude sites above,
           'rush' : True # processes as quickly as possible, submits to euro sites before raw data gets to stash
           }


dag = dag_writer(config)

#this name has to be changed in case one wants to do reprocessing
dag.write_outer_dag('/scratch/processing/dags_683/fei.dag')
