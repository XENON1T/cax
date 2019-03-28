from cax.dag_writer import dag_writer
import numpy as np
import pandas as pd
from make_runlist import make_runlist
from pax import __version__
import os

#runlist = make_runlist()#np.load('/home/ershockley/lowgains.npz')['numbers']
runlist = [16807, 17418, 17419, 23806]


config = { 'runlist' : runlist,
           'pax_version' :'v' +  __version__,
           'logdir' : '/scratch/processing',
           'retries' : 9,
           'specify_sites' : [],
 	   "exclude_sites": [], 
           'host' : 'login',
           'use_api': True,
           'use_midway' : False, # this overrides the specify and exclude sites above,
           'rush' : True # processes as quickly as possible, submits to euro sites before raw data gets to stash
           }


dag = dag_writer(config)

#this name has to be changed in case one wants to do reprocessing
dag.write_outer_dag('/scratch/ershockley/dags/retry_missingevents.dag')
