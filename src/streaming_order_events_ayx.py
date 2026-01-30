from influxdb import InfluxDBClient, DataFrameClient
import psycopg2
import pandas as pd
import numpy as np
import time
# from pandleau import *
import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction,postgres_connection
from pandasql import sqldf
import pytz
import os


class StreamingOrderEventsAyx():
    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db=self.tenant_info["out_db_name"])
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("streaming_order_events_ayx", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from streaming_order_events where time>='%s' and time<'%s' limit 1" %(check_start_date, check_end_date)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("streaming_order_events_ayx", '1d', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self, end_date, **kwargs):
        self.end_date = end_date
        self.start_date = self.client.get_start_date("streaming_order_events_ayx", self.tenant_info)
        self.start_date=pd.to_datetime(self.start_date)
        self.start_date=self.start_date.strftime('%Y-%m-%d')
        self.end_date = self.end_date.strftime('%Y-%m-%d')
        start_time=self.tenant_info['StartTime']
        start = self.start_date+"T"+start_time+"Z"
        end = self.end_date+"T"+start_time+"Z"
        q=f"select * from streaming_order_events where time>='%s' and time<'%s' " %(start, end)
        df['volume']= df['volume'].astype(float)
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                         port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "streaming_order_events_ayx", db_name=self.tenant_info["alteryx_out_db_name"],
                                                  tag_columns=None, dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])


with DAG(
    'streaming_order_events_ayx',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'streaming_order_events_ayx',
    schedule_interval = timedelta(hours=24),
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['streaming_order_events_ayx'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                streaming_order_events_ayx_final_task = PythonOperator(
                    task_id='StreamingOrderEventsAyx{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(StreamingOrderEventsAyx().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":"dev_postgres", "influx_ip":"dev_influxdb", "influx_port":8086,\
        #           "write_influx_ip":"dev_influxdb","write_influx_port":8086, \
        #           "out_db_name":"airflow", "alteryx_out_db_name": "airflow"}
        tenant = CommonFunction().get_tenant_info()
        streaming_order_events_ayx_final_task = PythonOperator(
            task_id='streaming_order_events_ayx_final',
            provide_context=True,
            python_callable=functools.partial(StreamingOrderEventsAyx().final_call,tenant_info={'tenant_info': tenant}),
            # op_kwargs={
            #     'tenant_info': tenant,
            # },
            execution_timeout=timedelta(seconds=3600),
        )
