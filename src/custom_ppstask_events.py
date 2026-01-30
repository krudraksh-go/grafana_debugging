## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient
import numpy as np
import pandas as pd
import ast

# from dags.utils.CommonFunction import InfluxData
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class CustomPpsTaskEvents:

    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        #        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("custom_ppstask_events", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")
        print(f"check start date : {check_start_date} check end date : {check_end_date}")

        q = f"select * from ppstask_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        print(q)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        print(df.shape)
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("custom_ppstask_events", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("custom_ppstask_events", '15min', self.tenant_info)
                print(daterange)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("custom_ppstask_events", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()

        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(self.start_date, self.end_date)
        if 'PROJECT' in self.tenant_info["Name"].upper() or 'HNMCANADA' in self.tenant_info["Name"].upper():
            q = f"select installation_id,pps_id, mtu_type_id ,profile from mtu_data  where  time >= '{self.start_date}' and time < '{self.end_date}' order by time desc"
        else:
            q = f"select installation_id,pps_id, profile, mtu_type_id from pps_data  where  time >= '{self.start_date}' and time < '{self.end_date}' order by time desc"

        order_df = pd.DataFrame(self.read_client.query(q).get_points())
        order_df = self.utilfunction.flow_event_for_profile(order_df, site_name=self.tenant_info["Name"])
        if not order_df.empty:
            order_df['time2'] = order_df.time.apply(
                lambda x: pd.to_datetime(x).replace(second=0, microsecond=0, nanosecond=0))
        q = f"select * from ppstask_events where  time >= '{self.start_date}' and time < '{self.end_date}' and task_type ='pick' and value<550000 order by time desc"
        ppstask_events_df = pd.DataFrame(self.read_client.query(q).get_points())

        if not ppstask_events_df.empty:
            ppstask_events_df['time2'] = ppstask_events_df.time.apply(
                lambda x: pd.to_datetime(x) + timedelta(minutes=5 - pd.to_datetime(x).minute % 5))
            ppstask_events_df['time2'] = ppstask_events_df.time2.apply(
                lambda x: x.replace(second=0, microsecond=0, nanosecond=0))
            df_final = pd.merge(ppstask_events_df, order_df[['installation_id', 'pps_id', 'time2', 'order_flow']],
                                on=['installation_id', 'pps_id', 'time2'])
            df_final.time = pd.to_datetime(df_final.time)
            df_final = df_final.set_index('time')
            df_final = df_final.drop(columns=['time2'])
            df_final['value'] = df_final['value'].astype(float, errors='ignore')
            print("inserted")
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_final, "custom_ppstask_events",
                                          db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['butler_id', 'host', 'installation_id', 'pps_id'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])


# -----------------------------------------------------------------------------
## Task defination
## -----------------------------------------------------------------------------
with DAG(
        'Custom_ppstask_events',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='',
        schedule_interval='*/15 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(minutes=60)
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['custom_ppstask_events'] == "Y":
                Client_setting_final_task = PythonOperator(
                    task_id='Custom_ppstask_events_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(CustomPpsTaskEvents().final_call,
                                                      tenant_info={'tenant_info': tenant}),
                )
                execution_timeout = timedelta(seconds=600),
    else:
        tenant = CommonFunction().get_tenant_info()
        Client_setting_final_task = PythonOperator(
            task_id='Custom_ppstask_events',
            provide_context=True,
            python_callable=functools.partial(CustomPpsTaskEvents().final_call, tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )
