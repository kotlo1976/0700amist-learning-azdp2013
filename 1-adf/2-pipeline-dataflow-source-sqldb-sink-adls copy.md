# Scenario: Create a Mapping DataFlow Pipeline using Azure Data Factory with Source as Azure SQL DB and Sink as Data Lake

Sol:

# Source - AZURE SQL DB
- Name of the SQL Server            : mainazsqlserver
- Name of the Azure SQL Database    : employeedb
- Name of the Employee table        : Employee
- Name of the Department table      : Department


# Sink - AZURE DATA LAKE STORAGE
- Name of the Sink(DataLake)        : ssadclouddatalake
- Name of the container             : output-from-dataflow-source-sqldb


# PIPELINES -> ACTIVITES -> DATASETS -> LINKED SERVICES

# 1.LINKED SERVCIES -> 2.DATASETS -> 3.ACTIVITY -> 4.PIPELINE


# LINKED SERVICES
- Name of the Source Linked Service : LSAzureSqlDatabase
- Name of the Sink Linked Service   : LS_AzureDataLakeStorage


# DATASETS
- Name of the Source Datasets       : DataSet_Source_Azsqldb_Employee
- Name of the Source Datasets       : DataSet_Source_Azsqldb_Department
- Name of the Sink Datasets         : Dataset_Sink_DataLakeStorage_EmployeewithDepartment

# DATAFLOW:
- Name of the Dataflow: Employee_Department_dataflow

# ACITVITY:
- Name of the Activity: dataflow-source-azsqldb-sink-datalake-activity

# PIPELINE:
- Name of the Pipeline: dataflow-source-azsqldb-sink-datalake-pipeline
