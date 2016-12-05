from cax.dag_writer import dag_writer

runlist = [3937, 3938, 3939]

logdir = "/home/ershockley/reprocessing_test/"
dag = dag_writer(runlist, "v6.1.1", logdir, reprocessing = True)

dag.write_outer_dag("/home/ershockley/reprocessing_test/outer.dag")

#dag.write_inner_dag(runlist[0], "inner.dag", "/xenon/ershockley/processed/", "processing_template.submit", "example.json")
