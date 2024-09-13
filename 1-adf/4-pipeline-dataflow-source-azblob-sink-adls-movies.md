# Scenario: Create a Pipeline for Transformed Dataset using MoviesDB from Azure Data Factory with Source as Azure Blob and Sink as Data Lake and Azure Mapping DataFlows

Sol:

# Source - AZURE BLOB STORAGE
- Name of the Source(Blob) : ssadcloudblobstorage
- Name of the container : myrawcontainer
- Name of the Blob      : moviesDB.csv


# Sink - AZURE DATA LAKE STORAGE
- Name of the Sink(DataLake)        : ssadclouddatalake
- Name of the container             : output-for-transformed-moviesdb



# PIPELINES -> ACTIVITES -> DATASETS -> LINKED SERVICES

# 1.LINKED SERVCIES -> 2.DATASETS -> 3.ACTIVITY -> 4.PIPELINE


# LINKED SERVICES
- Name of the Source Linked Service : LS_AzureBlobStorage
- Name of the Sink Linked Service   : LS_AzureDataLakeStorage


# DATASETS
- Name of the Source Datasets       : DataSet_Source_AzBlob_MoviesDB
- Name of the Sink Datasets         : Dataset_Sink_AzDataLake_MoviesRatingGreathan8

# DATAFLOW:
- Name of the Dataflow: moviesdb_with_ratinggreathan8_dataflow

# ACITVITY:
- Name of the Activity: moviesdb_with_ratinggreathan8_dataflow_activity

# PIPELINE:
- Name of the Pipeline: moviesdb_with_ratinggreathan8_dataflow_pipeline
