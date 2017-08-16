#from cax.dag_writer_mod import dag_writer
from cax.dag_writer import dag_writer
import numpy as np
from make_runlist import make_runlist
from pax import __version__
import os


os.environ["PYTHONPATH"]="/xenon/cax:"+os.environ["PYTHONPATH"]

#runlist=['170331_1249'] #MUV to reprocess
runlist=[11912] #paxv6.8.0
#runlist=[11914] #paxv6.8.0_OSG
#runlist = [6841]#, 6843, 6844, 6845, 6848, 6849, 6851, 6852, 6853, 6854, 6855, 6857, 6858, 6859, 6860, 6871, 6872, 6873, 6874, 6875, 6876, 6877, 6878, 6879, 6880, 6881, 6882, 6883, 6884, 6885, 6886, 7073, 7074, 7075, 7076, 7077, 7078, 7079, 7081, 7082, 7083, 7084, 7085, 7086, 7088, 7090, 7091, 7092, 7093, 7094, 7317, 7318, 7319, 7324, 7325, 7326, 7327, 7328, 7329, 7330, 7331, 7332, 7333, 7334, 7335, 7336, 7337, 7338, 7339, 7340, 7454, 7455, 7456, 7457, 7458, 7459, 7460, 7461, 7462, 7463, 7464, 7465, 7466, 7467, 7468, 7469, 7470, 7471, 7472, 7473, 7474, 7475, 7476, 7477, 7478, 7479, 7650, 7651, 7652, 7653, 7655, 7656, 7657, 7658, 7659, 7660, 7661, 7662, 7663, 7664, 7665, 7666, 7667, 7668, 7669, 7670, 7671, 7672, 7673, 7674, 7764, 7769, 8418, 8419, 8420, 8421, 8422, 8423, 8424, 8425, 8426, 8427, 8428, 8429, 8430, 8431, 8432, 8433, 8434, 8435, 8436, 8437, 8438, 8439, 8440, 8441, 8442, 8809, 8810, 8811, 8812, 8813, 8814, 8815, 8816, 8817, 8818, 8819, 8820, 8821, 8822, 8823, 8824, 8825, 8826, 8827, 8828, 8829, 8830, 8831, 8832, 8833] 

#runlist = make_runlist()
#runlist = sorted(runlist)



config = { 'runlist' : runlist,
           'pax_version' :'v' +  __version__,
           'logdir' : '/scratch/processing',
           'retries' : 9,
           'specify_sites' : [],
 	   "exclude_sites": ["Comet","WEIZMANN-LCG2","IN2P3-CC"], 
           'host' : 'login',
           'use_midway' : False, # this overrides the specify and exclude sites above,
           'rush' : True # processes as quickly as possible, submits to euro sites before raw data gets to stash
           }


dag = dag_writer(config)

#this name has to be changed in case one wants to do reprocessing
dag.write_outer_dag('/scratch/processing/pax6.8.0_notOSG_run_11912.dag')
#dag.write_outer_dag('/scratch/processing/pax6.6.2_MuV_run_170331_1249.dag')
#dag.write_outer_dag('/scratch/processing/forced_processing_666_old_kr85m_runs.dag')

