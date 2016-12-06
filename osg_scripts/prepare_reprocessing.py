from cax.dag_writer import dag_writer

runlist = range(2110, 2115)
logdir = "/xenon/ershockley/reprocessing"
dag = dag_writer(runlist, "v6.1.1", logdir, reprocessing = True)

dag.write_outer_dag(logdir + "/16Dec6.dag")
