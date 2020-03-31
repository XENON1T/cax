from cax.dag_writer import dag_writer
import numpy as np
import pandas as pd
from make_runlist import make_runlist
from pax import __version__
import os

dagpath = '/scratch/ershockley/dags/pax_v6.11.0'

os.makedirs(os.path.dirname(dagpath), exist_ok=True)

#runlist = [7289, 7356, 7362, 7429, 7439, 7453, 13047, 13050, 13155]
runlist = [23860, 18835, 18933] #np.arange(20008, 20127).tolist()
print(runlist)

config = { 'runlist' : runlist,
           'pax_version' :'v' +  __version__,
           'logdir' : '/scratch/processing',
           'retries' : 9,
           'specify_sites' : [],
 	   "exclude_sites": [], 
           'host' : 'login',
           'use_api': False,
           'use_midway' : False, # this overrides the specify and exclude sites above,
           'rush' : True # processes as quickly as possible, submits to euro sites before raw data gets to stash
           }


dag = dag_writer(config)

#this name has to be changed in case one wants to do reprocessing
dag.write_outer_dag(dagpath)
