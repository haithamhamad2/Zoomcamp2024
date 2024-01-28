#!/usr/bin/env python
# coding: utf-8

import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

parser = argparse.ArgumentParser(Description = 'Ingest CSV data to postgres')
# user
# pass
# host
# port
# database name
# table name
# url of the csv
parser.add_argument('user', help='user name for postgres')
parser.add_argument('pass', help='password for postgres')
parser.add_argument('host', help='host for postgres')
parser.add_argument('port', help='port for postgres')
parser.add_argument('db', help='database name for postgres')
parser.add_argument('table_name', help='name of table where we will write results to')
parser.add_argument('url', help='url of the csv file')
args = parser.parse_args()
print(args.accumulate(args.integers))


filename = '/home/Haitham.hamad/data-engineering-zoomcamp/01-docker-terraform/2_docker_sql/green_tripdata_2019-09.csv'

df_iter = pd.read_csv(filename,iterator=True,chunksize=100000)

df = next(df_iter)

engine.connect()

while True:
        t_start = time()
        df = next(df_iter)
        df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql('green_taxi_data', engine, if_exists='append')
        t_end = time()
        print('Insert another chunk, took %.3f' %(t_end-t_start))
print(pd.io.sql.get_schema(df, 'green_taxi_data', con=engine))



df_iter = pd.read_csv(filename,iterator=True,chunksize=100000)
# In[11]:


df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)


print(pd.io.sql.get_schema(df, 'green_taxi_data', con=engine))


df.head(n=0).to_sql(name='green_taxi_data',con=engine,if_exists='replace')


get_ipython().run_line_magic('time', "df.to_sql(name='green_taxi_data',con=engine,if_exists='append')")


