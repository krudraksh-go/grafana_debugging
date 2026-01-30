import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
import os
from config import (
    CONFFILES_DIR  )
class TaskCycleTimeProcessor:

    def update_idc_and_path_trvale_time(self, df, idx):
        if idx>0 and df['path_type'][idx]=='ttp_storable_io_point_to_ttp_storable_io_point' and df['path_type'][idx-1]=='highway_to_ttp_storable_io_point':
            curr_task_key = df['task_key'][idx]
            sum_idc = 0
            sum_path_travel = 0
            while idx>=0 and df['task_key'][idx] == curr_task_key and df['path_type'][idx]!='ttp_storable_io_point_to_highway':
                sum_idc += df['idc_time'][idx]
                sum_path_travel +=df['path_travel_time'][idx]
                idx = idx-1
            if idx>=0 and df['task_key'][idx]==curr_task_key and df['path_type'][idx] == 'ttp_storable_io_point_to_highway':
                return sum_idc+df['idc_time'][idx], sum_path_travel+df['path_travel_time'][idx]
        return 0,0

    def final_call(self,tenant_info,**kwargs):
        self.CommonFunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("task_cycle_time_transformed", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select * from  autogen.task_cycle_times where time>'{check_start_date}' and time<='{check_end_date}' and event_name = 'goto_completed' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("task_cycle_time_transformed", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("task_cycle_time_transformed", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self,end_date,**kwargs):
        self.start_date = self.client.get_start_date("task_cycle_time_transformed", self.tenant_info)
        self.utilfunction = CommonFunction()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select * from  autogen.task_cycle_times where time>'{self.start_date}' and time<='{self.end_date}' and event_name = 'goto_completed' "
        df = pd.DataFrame(self.read_client.query(q).get_points())
        
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "task_cycle_time_transformed"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return None

        if self.tenant_info['Name'] == 'HagerTTP2':
            two_day_before = (pd.to_datetime(self.start_date) - timedelta(hours=6)).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            two_day_before = (pd.to_datetime(self.start_date) - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}, two_day_before: {two_day_before}")

        q = f"select * from incomplete_task_cycle_times where time>'{two_day_before}' "
        incomplete_task_cycle_times = pd.DataFrame(self.client.query(q).get_points())

        if not incomplete_task_cycle_times.empty:
            df = pd.concat([df,incomplete_task_cycle_times])

        df['idc_time'] = df['idc_time'].apply(lambda x: 0 if x=='Not Set' else x)
        df['path_travel_time'] = df['path_travel_time'].apply(lambda x: 0 if x=='Not Set' else x)

        df['idc_time'] = df['idc_time'].astype(float,errors='ignore')
        df['path_travel_time'] = df['path_travel_time'].astype(float,errors='ignore')
        df['butler_id'] = df['butler_id'].astype(str, errors='ignore')
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df = self.CommonFunction.reset_index(df)

        q = f"select installation_id, task_key from ranger_task_events where time>='{self.start_date}' and time<='{self.end_date}' and event='task_completed' "

        completed_tasks = pd.DataFrame(self.read_client.query(q).get_points())
        if completed_tasks.empty:
            filter = {"site": self.tenant_info['Name'], "table": "task_cycle_time_transformed"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return None
        
        completed_tasks = completed_tasks[['installation_id','task_key']]
        completed_tasks = completed_tasks.groupby(['installation_id','task_key']).last()
        completed_tasks = completed_tasks.reset_index()
        df_1 = pd.merge(df,completed_tasks, on =['installation_id','task_key'], how='left', indicator=True)
        df = df_1[df_1['_merge']=='both']
        incomplete_cycles = df_1[df_1['_merge']=='left_only']
        del incomplete_cycles['_merge']
        del df['_merge']

        if not incomplete_cycles.empty:
            if 'index' in incomplete_cycles.columns:
                del incomplete_cycles['index']
            if 'level_0' in incomplete_cycles.columns:
                del incomplete_cycles['level_0']
            incomplete_cycles = incomplete_cycles.set_index('time')
            incomplete_cycles['deadlock_counts'] = incomplete_cycles['deadlock_counts'].astype(str)
            incomplete_cycles['deadlock_resolution_time'] = incomplete_cycles['deadlock_resolution_time'].astype(str)
            incomplete_cycles['total_path_travel_time'] = incomplete_cycles['total_path_travel_time'].astype(str)
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                            port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(incomplete_cycles, "incomplete_task_cycle_times", db_name=self.tenant_info["alteryx_out_db_name"],
                                              tag_columns=[],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "task_cycle_time_transformed"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return None


        df = df.sort_values(by=['installation_id','butler_id', 'task_key', 'time'], ascending=[True,True, True, True])
        df['new_idc_time'] = 0
        df['new_path_travel_time'] = 0

        df = self.CommonFunction.reset_index(df)
        if 'index' in df.columns:
            del df['index']
        df['index_col'] = df.index

        df[['new_idc_time','new_path_travel_time']] = df.apply(lambda x: self.update_idc_and_path_trvale_time(df, x['index_col']) , axis=1, result_type='expand' )
        df['new_idc_time'] = df.apply(lambda x: x['idc_time'] if x['new_idc_time']==0 else x['new_idc_time'],axis=1)
        df['new_path_travel_time'] = df.apply(lambda x: x['path_travel_time'] if x['new_path_travel_time']==0 else x['new_path_travel_time'], axis=1)
        cols = ['time','butler_id','event_name','installation_id','idc_time','new_idc_time','path_travel_time','new_path_travel_time','path_type','pps_id','task_key','task_type']
        df = df[cols]
        df['time'] = pd.to_datetime(df['time'],utc=True)
        df = df.set_index('time')
        df['new_idc_time'] = df['new_idc_time'].astype(float)
        df['new_path_travel_time'] = df['new_path_travel_time'].astype(float)
        df['idc_time'] = df['idc_time'].astype(float)
        df['path_travel_time'] = df['path_travel_time'].astype(float)
        df['path_type'] = df['path_type'].astype(str)
        df['event_name'] = df['event_name'].astype(str)
        df['pps_id'] = df['pps_id'].astype(str)
        df['butler_id'] = df['butler_id'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)
        df['task_key'] = df['task_key'].astype(str)
        df['task_type'] = df['task_type'].astype(str)

        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "task_cycle_time_transformed"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return None

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "task_cycle_time_transformed", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['path_type','butler_id'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'task_cycle_time_transform',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Update IDC Mean and Path Travel Time',
    schedule_interval = '*/15 * * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False,
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['task_cycle_time_transform'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Task_cycle_time_final_task = PythonOperator(
                    task_id='task_cycle_time_transform_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(TaskCycleTimeProcessor().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        tenant = CommonFunction().get_tenant_info()
        Task_cycle_time_final_task = PythonOperator(
            task_id='task_cycle_time_transform_final',
            provide_context=True,
            python_callable=functools.partial(TaskCycleTimeProcessor().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )