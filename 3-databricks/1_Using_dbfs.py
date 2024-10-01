# Databricks notebook source
# Create empDF with Workspace data/emp.parquet
empDF = spark.read.parquet('data/emp.parquet')

# COMMAND ----------

# Upload data/emp.parquet to shared storage - /FileStore/shared_uploads/iriscloudone@outlook.com/emp.parquet
filePath = 'dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/emp.parquet'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# List all the objects from given file path
display(dbutils.fs.ls("/FileStore/shared_uploads/iriscloudone@outlook.com/"))

# COMMAND ----------

# List Databricks Datasets for Practisce
display(dbutils.fs.ls("dbfs:/databricks-datasets"))

# COMMAND ----------

# Create a directory
dbutils.fs.mkdirs("dbfs:/sid")

# COMMAND ----------

dbutils.fs.ls("dbfs:/sid")

# COMMAND ----------

# Copy emp.parquet from Shared Storage to the directory created
dbutils.fs.cp("dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/emp.parquet","dbfs:/sid/emp.parquet")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/sid")) 

# COMMAND ----------

empDF = spark.read.parquet("dbfs:/sid/emp.parquet")

# COMMAND ----------

display(empDF)

# COMMAND ----------

display(dbutils.fs.rm('dbfs:/sid/emp.parquet'))
