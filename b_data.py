#!/usr/bin/env python
# coding: utf-8

import os
from time import time
import pandas as pd
import argparse

from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name
    csv_name='output.csv'
    #download csv
    os.system(f'wget {url} -O {csv_name}')
    #establish connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100000)

    df = next(df_iter)
   
    #place the table names ino postgres
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    #append the rest of the csv data into the database
    df.to_sql(name=table_name, con=engine, if_exists='append')
    while True:
        t_start = time()
        df = next(df_iter)
    
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('inserted another chunk..., took %.3f seconds' %(t_end - t_start))
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ingest csv to postgres')
    
    parser.add_argument('--user', help='postgres user name')
    parser.add_argument('--password', help='postgres password')
    parser.add_argument('--host', help='postgres host')
    parser.add_argument('--port', help='postgres port')
    parser.add_argument('--db', help='postgres db name')
    parser.add_argument('--table_name', help='postgres table name for the results')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)






