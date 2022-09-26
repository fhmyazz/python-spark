#!/usr/bin/python3

from calendar import month
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql.functions import month


import os
import json
import sqlparse

import pandas as pd
import numpy as np

import connection
import conn_warehouse

if __name__ == '__main__':
    filetime = datetime.now().strftime('%Y%m%d')
    print(f"[INFO] Service ETL is Starting .....")

    #connect db warehouse
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect db source
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    #connect spark
    conf = connection.config('spark')
    spark = connection.spark_conn(app="ETLDigitalskola",config=conf)

    #query extract db source
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            path_query+'query.sql','r'
            ).read(), strip_comments=True).strip()

    #query load db warehouse
    query_dwh = sqlparse.format(
        open(
            path_query+'dwh_design.sql','r'
            ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)
        print(f"{df}")

        # spark processing
        sparkDF = spark.createDataFrame(df)
        print(f"{sparkDF}")
        sparkDF.groupBy(month("order_date")).sum("order_total") \
            .toPandas() \
                .to_csv(f"/mnt/d/digitalskola/prj5/live/output.csv", index=False)

        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")
    

    