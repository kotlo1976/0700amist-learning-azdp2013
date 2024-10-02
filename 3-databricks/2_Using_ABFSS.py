# Databricks notebook source
# https://docs.databricks.com/en/connect/storage/azure-storage.html#language-Account%C2%A0key
# https://ssadcloudazsynapsedl2.blob.core.windows.net/datalakefs/output-from-source-blob/emp.parquet
# spark.read.load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Connect with Azure Data Lake Storage using ABFSS

# COMMAND ----------

# Assign with your Data Lake Values
storage_account_name = 'ssadcloudazsynapsedl2'
container_name = 'datalakefs'
path_to_data = 'output-from-source-blob/emp.parquet'
storage_account_access_key = '<access-key-here>'

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_access_key
)

# COMMAND ----------

abfss_file_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path_to_data}"

# COMMAND ----------

print(abfss_file_path)

# COMMAND ----------

empDF = spark.read.parquet(abfss_file_path)

# COMMAND ----------

display(empDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS emp_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM emp_table

# COMMAND ----------

# COPY INTO <database-name>.<table-name>
# FROM 'abfss://container@storageAccount.dfs.core.windows.net/path/to/folder'
# FILEFORMAT = CSV
# COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO  emp_table
# MAGIC FROM  'abfss://datalakefs@ssadcloudazsynapsedl2.dfs.core.windows.net/output-from-source-blob/emp.parquet'
# MAGIC FILEFORMAT = Parquet
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

display(spark.sql('SELECT * FROM emp_table'))
