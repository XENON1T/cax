from cax.dag_writer import dag_writer
import numpy as np

#runlist = np.arange(3500, 4000) # np.arange(3501, 4000) 
runlist = np.arange(4000, 4003)
logdir = "/xenon/ershockley/reprocessing"
dag = dag_writer(runlist, "v6.1.1", logdir, reprocessing = True)

dag.write_outer_dag(logdir + "/16Dec15_splicetest.dag")
