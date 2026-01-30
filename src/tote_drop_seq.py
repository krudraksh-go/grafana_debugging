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

class ToteSequence:

    def final_call(self,tenant_info,**kwargs):
        self.CommonFunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("tote_sequence", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from tote_drop_seq_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("tote_sequence", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("tote_sequence", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self,end_date,**kwargs):
        self.start_date = self.client.get_start_date("tote_sequence", self.tenant_info)
        self.utilfunction = CommonFunction()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.original_start = pd.to_datetime(self.start_date)-timedelta(minutes=30)
        self.original_start = self.original_start.strftime('%Y-%m-%dT%H:%M:%SZ')

        print(f"start_date: {self.start_date}, end_date:{self.end_date}")

        q = f"SELECT * FROM tote_drop_seq_events where time>='{self.start_date}' and time<='{self.end_date}' "
        df_tote_drop_seq = pd.DataFrame(self.read_client.query(q).get_points())

        if df_tote_drop_seq.empty:
            filter = {"site": self.tenant_info['Name'], "table": "tote_sequence"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return None

        df_tote_drop_seq.rename(columns={'butler_id': 'ranger_id'}, inplace=True)
        df_tote_drop_seq['ranger_id'] = df_tote_drop_seq['ranger_id'].astype(str)
        col = df_tote_drop_seq.columns
        q = f"select * from tote_sequence where time>='{self.original_start}' and time<='{self.end_date}'"
        df_tote_sequence = pd.DataFrame(self.client.query(q).get_points())

        if not df_tote_sequence.empty:
            if 'unload_time' in df_tote_drop_seq.columns:
                df_tote_sequence = df_tote_sequence[pd.isna(df_tote_sequence['unload_time'])].reset_index()
                del df_tote_sequence['unload_time']
            if 'index' in df_tote_sequence.columns:
                del df_tote_sequence['index']
            df_tote_sequence = df_tote_sequence[col]
            df_tote_drop_seq = pd.concat([df_tote_drop_seq,df_tote_sequence])

        q = f"SELECT task_key as rgt_key,entity_id as tote_id ,installation_id,ranger_id FROM ranger_task_events WHERE event = 'entity_unloaded' and time>'{self.start_date}' and time<'{self.end_date}' and task_status ='unloading_at_conveyor' "
        df_ranger_task_events = pd.DataFrame(self.read_client.query(q).get_points())
        if df_ranger_task_events.empty:
            cols = ['time','rgt_key','tote_id','installation_id','ranger_id']
            df_ranger_task_events = pd.DataFrame(columns=cols)
        df_ranger_task_events.rename(columns={'time': 'unload_time'}, inplace=True)
        df_ranger_task_events['ranger_id'] = df_ranger_task_events['ranger_id'].astype(str)
        df = pd.merge(df_tote_drop_seq, df_ranger_task_events, on=['installation_id','rgt_key','tote_id','ranger_id'], how='left')
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df = df.set_index('time')

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "tote_sequence", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['rgt_key'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        
        print("inserted")
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'tote_sequenece',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Tote Sequence',
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
            if tenant['Active'] == "Y" and tenant['tote_sequenece'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='tote_sequenece_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(ToteSequence().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='tote_sequenece_final',
            provide_context=True,
            python_callable=functools.partial(ToteSequence().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )