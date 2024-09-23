# Scenario: Create a COPY Pipeline using Azure Synapse with Source as BLOB and Sink as Data Lake

Sol:

# Source - AZURE BLOB STORAGE
- Name of the Source(Blob) : ssadcloudblobstorage
- Name of the container : myrawcontainer


# Sink - AZURE DATA LAKE STORAGE
- Name of the Sink(DataLake) : ssadcloudazsynapsedl2
- Name of the container      : datalakefs / output-from-source-blob



# PIPELINES -> ACTIVITES -> DATASETS -> LINKED SERVICES

# 1.LINKED SERVCIES -> 2.DATASETS -> 3.ACTIVITY -> 4.PIPELINE


# LINKED SERVICES
- Name of the Source Linked Service : LS_AzureBlobStorage
- Name of the Sink Linked Service   : LS_AzureDataLakeStorage


# DATASETS
- Name of the Source Datasets : Dataset_Source_Blob_Employee
- Name of the Sink Datasets : Dataset_Sink_DataLakeStorage_Employee


# ACITVITY:
- Name of the Activity: cp-source-blob-sink-datalake-activity


# PIPELINE:
- Name of the Pipeline: cp-source-blob-sink-datalake-pipeline
