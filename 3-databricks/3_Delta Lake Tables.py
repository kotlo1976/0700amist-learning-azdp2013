# Databricks notebook source
# Use with associated Data Lake Storage with Azure Databricks -dbfs
# dbfs:/FileStore/shared_uploads/iriscloudone@outlook.com/greentaxi.parquet

# Use with External Data Lake Storage with Azure Datarbricks - abfss
# abfss://datalakefs@ssadcloudazsynapsedl2.dfs.core.windows.net/greentaxi/greentaxi.parquet


# COMMAND ----------

# Assign with your Data Lake Values
storage_account_name = 'ssadcloudazsynapsedl2'
container_name = 'datalakefs'
path_to_data = 'greentaxi/greentaxi.parquet'
storage_account_access_key = 'Key'

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_access_key
)

# COMMAND ----------

abfss_greentaxi_file_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{path_to_data}"

# COMMAND ----------

print(abfss_greentaxi_file_path)

# COMMAND ----------

greentaxiDF = spark.read.parquet(abfss_greentaxi_file_path)

# COMMAND ----------

#d f.write.format("delta").mode("overwrite").save("/tmp/delta/people10m")

# COMMAND ----------

curatedgreentaxiDF = greentaxiDF['VendorID','lpep_pickup_datetime','lpep_dropoff_datetime','passenger_count','fare_amount']

# COMMAND ----------

display(curatedgreentaxiDF)

# COMMAND ----------

#df.write.format("delta").mode("overwrite").save("/tmp/delta/people10m")
curatedgreentaxiDF.write.format("delta").mode("overwrite").save("/tmp/delta/greentaxi")

# COMMAND ----------

newcuratedgreentaxiDF = spark.read.format("delta").load("/tmp/delta/greentaxi")

# COMMAND ----------

display(newcuratedgreentaxiDF)
