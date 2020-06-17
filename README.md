# trapum_new_pipeline
The default TRAPUM pipeline through a command line interface for the TRAPUM cluster

The pipeline is split into 4 parts: sending a processing request, searching, folding and scoring. Each part communicates with the RabbitMQ service and processes the data. At every step, a centralised database is used to insert/update/delete metainformation about the chain that helps track every single entity going in and produced from the pipeline. Sample command for each step is given in each directory. The necessary Dockerfile required to run this process is also available in this directory.

The name **trapum_new** is used to refer to the name of the database used for running this pipeline
