# Batch_ETL_Snowpark_Synch
This shows an example of using a Python Script to upload periodic changes from a source datasource directly into Snowflake using Snowpark.  This is a direct transfer, without having to generate and shuffle files for loading.  This is basic raw data ingestion, can then do ELT processing in Snowflake to further refine the data to suite your business needs.

##Assumptions
1. Each source system's table/object has a unique identifier for performing a merge operation in Snowflake
2. Each source system's table/object has a "last updated" timestamp field to identify objects that have been added/updated since the last sychronization run.
3. This currently is not processing any hard deletes.
4. In the ETL_HISTORY table in Snowflake, you list out the tables you want to sychronize.  You also pre-create a replica table in Snowflake that mirrors the source with the same name.
5. This is a simple script, which runs sequentially and in your python environment for incremental loading.  This is more of a sample and starting point than a fully-productionized ingestion framework.
