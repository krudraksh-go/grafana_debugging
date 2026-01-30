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
                                             host="192.168.213.201",
                                             port="5432",
                                             database="butler_interface")
print("\nConnecting to GreyOrange Interface successful\n")
run_query_Test()
print("\nDone\n")
