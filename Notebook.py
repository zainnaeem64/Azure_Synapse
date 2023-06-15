#!/usr/bin/env python
# coding: utf-8

# ## Notebook 1
# 
# 
# 

# In[2]:


get_ipython().run_cell_magic('pyspark', '', "df = spark.read.load('abfss://housepricing@datalakezain.dfs.core.windows.net/input/housing_in_london_monthly_variables.csv', format='csv'\r\n## If\u202fheader\u202fexists\u202funcomment\u202fline\u202fbelow\r\n, header=True\r\n)\r\ndisplay(df.limit(15))\n")


# In[27]:


import pyspark.sql.functions as f
from pyspark.sql.types import *


## Adding year column
df=df.withColumn('years',f.year(f.to_timestamp('date', 'yyyy-MM-dd')))


df.printSchema()

## Changing data type of coloums
df=df.withColumn('houses_sold',df['houses_sold'].cast(FloatType()))
df=df.withColumn('average_price',df['average_price'].cast(FloatType()))

df.printSchema()


# In[30]:


##creating yearly data of houses sold 

yearly_data=df.groupby('years','area').sum('houses_sold')

yearly_data.show(1)


# In[42]:


## checking unique areas
yearly_data.select('area').distinct().count()


# In[45]:


##creating yearly data of average price

yearly_data2=df.groupby('years','area').avg('average_price')
yearly_data2.count()


# In[48]:


## joining 2 dataframes 

yearly_data=yearly_data2.join(yearly_data,['years','area'])


# In[50]:


# Populate a temporary view so we can query from SQL

yearly_data.createOrReplaceTempView("yearly_data")


# In[56]:


get_ipython().run_cell_magic('sql', '', "\r\nselect * from yearly_data\r\nwhere area='london' \r\norder by years asc \r\nlimit 5\n")


# In[57]:


get_ipython().run_cell_magic('pyspark', '', "\r\nyearly_data.write.parquet('abfss://housepricing@datalakezain.dfs.core.windows.net/input/yearly_data'+'.parquet') \r\n\r\n")


# In[58]:


a=spark.read.load('abfss://housepricing@datalakezain.dfs.core.windows.net/input/yearly_data.parquet', header=True)


# In[59]:


a.show(1)

