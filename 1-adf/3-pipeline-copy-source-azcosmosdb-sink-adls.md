# Scenario: Create a COPY Pipeline using Azure Data Factory with Source as Azure COSMOS DB and Sink as Data Lake

Sol:

# Source - AZURE COSMOSDB
- Name of the Azure CosmosDB Account    : maincosmosdb
- Name of the Databases                 : employeeDB
- Name of the Container                 : employeeContainer


# Sink - AZURE DATA LAKE STORAGE
- Name of the Sink(DataLake)        : ssadclouddatalake
- Name of the container             : output-from-copy-source-cosmosdb


# PIPELINES -> ACTIVITES -> DATASETS -> LINKED SERVICES

# 1.LINKED SERVCIES -> 2.DATASETS -> 3.ACTIVITY -> 4.PIPELINE


# LINKED SERVICES
- Name of the Source Linked Service : LSCosmosDbNoSql
- Name of the Sink Linked Service   : LS_AzureDataLakeStorage


# DATASETS
- Name of the Source Datasets       : DataSet_Source_AzCosmosdb_Employee
- Name of the Sink Datasets         : Dataset_Sink_DataLakeStorage_EmployeeComsodb

# DATAFLOW:
- Name of the Dataflow: Employee_Department_dataflow

# ACITVITY:
- Name of the Activity: cp-source-azcosmosdb-sink-datalake-activity

# PIPELINE:
- Name of the Pipeline: cp-source-azcosmosdb-sink-datalake-pipeline
