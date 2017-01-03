from cax.dag_writer import dag_writer
import numpy as np

#runlist = np.arange(3500, 4000) # np.arange(3501, 4000) 
runlist = np.arange(2010, 5900)
logdir = "/xenon/ershockley/reprocessing"
dag = dag_writer(runlist, "v6.1.1", logdir, reprocessing = True)
dag.write_outer_dag(logdir + "/17Jan02_BIG.dag")
