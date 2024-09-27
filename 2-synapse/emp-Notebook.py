#!/usr/bin/env python
# coding: utf-8

# ## emp-Notebook
# 
# 
# 

# In[6]:


get_ipython().run_cell_magic('pyspark', '', "df = spark.read.load('abfss://datalakefs@ssadcloudazsynapsedl2.dfs.core.windows.net/output-from-source-blob/emp.parquet', format='parquet')\r\ndisplay(df.limit(10))\n")


# In[7]:


# empDF = spark.read.parquet('https://ssadcloudazsynapsedl2.blob.core.windows.net/datalakefs/output-from-source-blob/emp.parquet')


# In[ ]:


#df = spark.read.load('abfss://datalakefs@ssadcloudazsynapsedl2.dfs.core.windows.net/output-from-source-blob/emp.parquet', format='parquet')
empDF = spark.read.parquet('abfss://datalakefs@ssadcloudazsynapsedl2.dfs.core.windows.net/output-from-source-blob/emp.parquet')


# In[11]:


df.createOrReplaceTempView('empwithdepTable')
#empwithdepDF.createOrReplaceTempView('empwithdepTable')


# In[13]:


spark.sql('SELECT * FROM empwithdepTable')


# In[14]:


spark.sql('SELECT AVG(emp_salary) AS avgSalarybyDep,emp_department FROM empwithdepTable GROUP BY emp_department').show()

