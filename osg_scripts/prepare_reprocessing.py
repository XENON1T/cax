from cax.dag_writer import dag_writer
import numpy as np

runlist = np.arange(2600, 3500) #np.arange(2819, 3000) 
logdir = "/xenon/ershockley/reprocessing"
dag = dag_writer(runlist, "v6.1.1", logdir, reprocessing = True)

dag.write_outer_dag(logdir + "/16Dec11.dag")
