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
class R2rWaittimeCalculation:

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

        check_start_date = self.client.get_start_date("r2r_waittime_calculation", self.tenant_info)
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
            self.final_call1(self.end_date,df.empty, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("r2r_waittime_calculation", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("r2r_waittime_calculation", '5min', self.tenant_info)
                print(daterange)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date,df.empty ,**kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
    def final_call1(self, end_date, emptyflag,**kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("r2r_waittime_calculation", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()

        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(self.start_date, self.end_date)
        q = f"select * from ppstask_events where  time >= '{self.start_date}' and time < '{self.end_date}'  and (event = 'rack_started_to_depart_pps' or event = 'rack_arrived_to_pps')  order by time desc"
        ppstask_events_df = pd.DataFrame(self.read_client.query(q).get_points())

        if not ppstask_events_df.empty and not emptyflag:
            if 'previous_pps_state' in ppstask_events_df.columns:
                ppstask_events_df=ppstask_events_df[(ppstask_events_df['previous_pps_state']=='active')]

            if 'rack_presented' in ppstask_events_df.columns:
                ppstask_events_df=ppstask_events_df[(ppstask_events_df['rack_presented']=='yes') | (ppstask_events_df['event']=='rack_started_to_depart_pps')]

            ppstask_events_df.time = pd.to_datetime(ppstask_events_df.time)
            ppstask_events_df = ppstask_events_df.set_index('time')
            ppstask_events_df['value'] = ppstask_events_df['value'].astype(float, errors='ignore')
            ppstask_events_df=ppstask_events_df[['host','installation_id','pps_id','task_type','value','event']]
            print("inserted")
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(ppstask_events_df, "r2r_waittime_calculation",
                                          db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['installation_id','pps_id'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        else:
            filter = {"site": self.tenant_info['Name'], "table": "r2r_waittime_calculation"}
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter, new_last_run)


# # -----------------------------------------------------------------------------
# ## Task defination
# ## -----------------------------------------------------------------------------
# with DAG(
#         'R2r_Waittime_Calculation',
#         default_args=CommonFunction().get_default_args_for_dag(),
#         description='',
#         schedule_interval='*/15 * * * *',
#         max_active_runs=1,
#         max_active_tasks=16,
#         concurrency=16,
#         catchup=False
# ) as dag:
#     import csv
#     import os
#     import functools
#
#     if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
#         csvReader = CommonFunction().get_all_site_data_config()
#         for tenant in csvReader:
#             if tenant['Active'] == "Y" and tenant['Picks_per_rack_face'] == "Y":
#                 Client_setting_final_task = PythonOperator(
#                     task_id='R2r_Waittime_Calculation_final_{}'.format(tenant['Name']),
#                     provide_context=True,
#                     python_callable=functools.partial(R2rWaittimeCalculation().final_call,
#                                                       tenant_info={'tenant_info': tenant}),
#                 execution_timeout = timedelta(seconds=600),)
#     else:
#         tenant = CommonFunction().get_tenant_info()
#         Client_setting_final_task = PythonOperator(
#             task_id='R2r_Waittime_Calculation',
#             provide_context=True,
#             python_callable=functools.partial(R2rWaittimeCalculation().final_call, tenant_info={'tenant_info': tenant}),
#             op_kwargs={
#                 'tenant_info1': tenant,
#             },
#             execution_timeout=timedelta(seconds=3600),
#         )
