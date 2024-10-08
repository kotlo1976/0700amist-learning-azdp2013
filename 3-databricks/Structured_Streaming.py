# Databricks notebook source
# dbfs:/FileStore/shared_uploads/kumar@vcloudmatesolutions.com/products.csv
# dbfs:/FileStore/shared_uploads/kumar@vcloudmatesolutions.com/sales_orders.csv

# COMMAND ----------

sales_ordersDF = spark.read.csv('dbfs:/FileStore/shared_uploads/kumar@vcloudmatesolutions.com/sales_orders.csv',header=True,inferSchema=True)
productsDF     = spark.read.csv('dbfs:/FileStore/shared_uploads/kumar@vcloudmatesolutions.com/products.csv',header=True,inferSchema=True)

# COMMAND ----------

display(sales_ordersDF)

# COMMAND ----------

display(productsDF)

# COMMAND ----------

display(sales_ordersDF.isStreaming)
display(productsDF.isStreaming)

# COMMAND ----------

sales_ordersDF.write.format('parquet').save('/tmp/salesorders')
productsDF.write.format('parquet').save('/tmp/products')

# COMMAND ----------

sales_orders_StreamingDF = spark.readStream.format('parquet') \
                                            .schema(sales_ordersDF.schema) \
                                            .load('/tmp/salesorders')

# COMMAND ----------

products_StreamingDF = spark.readStream.format('parquet') \
                                            .schema(productsDF.schema) \
                                            .load('/tmp/products')

# COMMAND ----------

display(sales_orders_StreamingDF.isStreaming)
display(products_StreamingDF.isStreaming)

# COMMAND ----------

join_sales_orders_products_StreamingDF = sales_orders_StreamingDF.join(products_StreamingDF,'product_id')

# COMMAND ----------

#q = df.writeStream.format("console").start()
#time.sleep(3)
#q.stop()

# COMMAND ----------

# query = join_sales_orders_products_StreamingDF.writeStream.format('console').start()

# COMMAND ----------

source_checkpointLocation = '/tmp/_checkpoint'
tableName = 'StreamingSalesOrderProductsTable'

# COMMAND ----------

query = join_sales_orders_products_StreamingDF.writeStream.format('parquet') \
                                                .option("checkpointLocation",source_checkpointLocation) \
                                                .outputMode('append') \
                                                .table(tableName)
                                                
                                                

# COMMAND ----------

totalSalesQuery ='SELECT SUM(total_amount) FROM streamingsalesorderproductstable'

# COMMAND ----------

totalSalesQueryResults = spark.sql(totalSalesQuery)

# COMMAND ----------

display(totalSalesQueryResults)

# COMMAND ----------

# query.awaitTermination()
