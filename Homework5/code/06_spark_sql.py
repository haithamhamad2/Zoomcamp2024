#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[4]:


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://zoomcamp2024.us-west1-b.c.sonic-airfoil-411903.internal:7077") \
    .appName('test') \
    .getOrCreate()


# In[5]:


#instead of local we will call the standalone port
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


# In[6]:


df_green = spark.read.parquet('data/pq/green/*/*')


# In[7]:


spark


# In[8]:


df_green.printSchema()


# In[9]:


df_green.columns


# In[10]:


df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


# In[11]:


df_yellow = spark.read.parquet('data/pq/yellow/*/*')


# In[12]:


df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


# In[13]:


df_yellow.printSchema()


# In[14]:


df_yellow.columns


# In[15]:


set(df_green.columns) & set(df_yellow.columns)  #shows common columns


# In[16]:


#to reserve order of columns
common_colums = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_colums.append(col)


# In[17]:


common_colums


# In[18]:


df_green.select(common_colums).show()


# In[19]:


from pyspark.sql import functions as F


# In[20]:


#Add service type column to differentiate the data when merged
df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))


# In[21]:


#Add service type column to differentiate the data when merged
df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))


# In[22]:


df_trips_data = df_green_sel.unionAll(df_yellow_sel)


# In[23]:


df_trips_data.groupBy('service_type').count().show()


# In[24]:


df_trips_data.columns


# In[25]:


df_trips_data.registerTempTable('trips_data')


# In[26]:


spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY 
    service_type
""").show()


# In[27]:


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


# In[28]:


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('hour', pickup_datetime) AS revenue_hourly, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


# In[ ]:


df_result.show()


# In[24]:


df_result.coalesce(2).write.parquet('data/report/revenue/', mode='overwrite')  #one in coalesce will reduce partitions and write 1 file. This is written to data lake


# In[ ]:




