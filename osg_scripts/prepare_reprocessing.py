#from cax.dag_writer_mod import dag_writer
from cax.dag_writer import dag_writer
import numpy as np
from make_runlist import make_runlist
from pax import __version__



runlist = list(np.arange(2633, 2655)) + list(np.arange(3144, 3169)) + list(np.arange(3472, 3496)) + list(np.arange(4545, 4569))

runlist = [int(run) for run in runlist ]
runlist = sorted(runlist)

config = { 'runlist' : runlist,
           'pax_version' :'v' +  __version__,
           'logdir' : '/scratch/processing',
           'retries' : 9,
           'specify_sites' : [],
           'exclude_sites' : ['Comet'],
           'host' : 'login',
           'use_midway' : False, # this overrides the specify and exclude sites above,
           'rush' : True # processes as quickly as possible, submits to euro sites before raw data gets to stash
           }


dag = dag_writer(config)
dag.write_outer_dag('/scratch/processing/665_murra_4dates.dag')

