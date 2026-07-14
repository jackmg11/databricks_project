## Project Overview

This ETL project has been built using databricks free edition and takes a look at windfarm data. CSV files have been placed at this location '/Volumes/workspace/files/raw_files'

This project follows Medallion Architecture. Bronze loads the raw CSV files into one table while enhancing the raw data with relevant metadata.
being that our source is append only our bronze notebook only checks for rows which do not exist in the current bronze table after the initial load.

Silver follows the same delta load pattern from the bronze table but also pushes anomaly records to the anomaly table for further analysis

Gold also uses the delta load pattern from the silver table and the data is curated at a grain level of one record per day and turbine ID and provides requested stats.

The way in which our ETL has been built assumes no changes will be made to old records (once processed a record with the same turbine_id and timestamp will not be reprocessed)

Job clusters not specified in yml file due to it being run on serverless (free edition only allows serverless)
## Project set up 

you can either deploy this VIA the databricks UI or through local deployment using databrickCLI

this can be done by installing databricksCLI - winget install Databricks.DatabricksCLI or brew install databricks depending on your OS.

after installation run databricks auth login --host https://<your-workspace-url>

on your databricks workspace go to Settings then Developer then Access tokens and generate a new access token. seeing as this is a creative project I have granted my API scope to all-apis (this is not recommended and has since been revoked)

you can then run databricks configure --token in your terminal and provide workspace url and then your access token.

This will allow you to run a databricks bundle validate command and if successful then a databricks bundle deploy -t dev command

due to this being deployed on the free version of databricks catalog creation via DAB Deployment isn't possible so it is created via the set_up notebook which runs at the start of our job run before our ETL notebooks (schemas are also created here).

The way the job has been built is with the intention that it is ran before starting any further work on this ETL as the set up and notebooks are responsible for creating the schemas and tables (This can be run in the notebooks following each tasks dependency from the job)  

## Future improvements
 
For this to be deployed to a production environment, a number of source clarifications should be made, and the ETL should be enhanced based on those clarifications. A lot of duplicated code in these notebooks should really be implemented via functions for reusability across different projects/notebooks. The final section of the testing notebook, which adds new random records to a dummy source to test the delta loading method, should ideally be implemented as a separate function rather than being written in the same fashion, the delta load itself should also be implemented in a more efficient way once the requirements are better understood (for example, whether historical records will ever be corrected or updated, and how those changes should be handled).