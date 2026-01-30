#!/usr/bin/python3

from influxdb import InfluxDBClient, DataFrameClient
import psycopg2
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import time
from pandleau import *






def run_query_Test():
    connection = psycopg2.connect(user="altryx_read",
                                         password="YLafmbfuji$@#45!",
                                         host="172.30.252.230", # change
                                         port="8087", # change
                                         database="tower")
    cursor  = connection.cursor()
    cursor.execute("SELECT * FROM auth_user limit 10")
    data = cursor.fetchall()
    df1=pd.DataFrame(data)
    print(df1)

run_query_Test()
