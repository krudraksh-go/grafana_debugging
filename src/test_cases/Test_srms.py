#!/usr/bin/python3
import psycopg2
import pandas as pd
import numpy as np
from datetime import timedelta,datetime
import time
from influxdb import DataFrameClient,InfluxDBClient

def run_query_Test():
        connection_Test = psycopg2.connect(user="altryx_read",
                                             password="YLafmbfuji$@#45!",
                                             host="172.19.40.74",
                                             port="5435",
                                             database="platform_srms")
print("\nConnecting to GreyOrange SRMS successful\n")
run_query_Test()
print("\nDone\n")