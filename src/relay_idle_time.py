## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction

## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class RelayIdleTime:
    def save_last_entry(self):
        filter = {"site": self.tenant_info['Name'], "table": "relay_idle_time"}
        new_last_run = {"last_run": self.end_date}
        self.CommonFunction.update_dag_last_run(filter, new_last_run)
        return None   

    def fetch_raw_data(self):
        fetch_condition = f"((event = 'task_completed' or event = 'task_assigned') and ranger_type = 'htm') or ((event = 'subtask_completed' or event = 'subtask_started') and ranger_type = 'vtm')"
        q = f"select * from ranger_events where time>'{self.start_date}' and time <= '{self.end_date}' and ({fetch_condition}) order by time"
        df = pd.DataFrame(self.read_client.query(q).get_points()) 
        selected_cols = ['time','event','ranger_version','ranger_type','task_key','ranger_id','installation_id','host','subtask_key','final_destination_location']
        if df.empty:
            return pd.DataFrame(columns = selected_cols)
        df = df[selected_cols]
        df['event'] = df['event'].replace('subtask_started', 'task_assigned')
        df['event'] = df['event'].replace('subtask_completed', 'task_completed')
        df = df.fillna('')
        df = df.replace({'nan': ''})
        df = df.replace({'null': ''})
        df['time'] = pd.to_datetime(df['time'])
        print("Data Fetched!!")
        return df                 

    def create_cols(self, df):
        event_cols = ['task_assigned','task_completed']
        for col in event_cols:
            df[col+'_time'] = np.nan
            df[col+'_time'] = df.apply(lambda x : x['time'] if x['event']==col else np.nan, axis=1)
        return df

    def group_data(self, df):
        df['task_assigned_time'] = pd.to_datetime(df['task_assigned_time'], utc=True)
        df['task_completed_time'] = pd.to_datetime(df['task_completed_time'], utc=True)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df = df.groupby(['host', 'installation_id','ranger_type','ranger_version', 'ranger_id', 'task_key','subtask_key'], as_index=False).agg(
            time = ('time', 'min'),
            task_assigned_time = ('task_assigned_time', 'min'),
            task_completed_time = ('task_completed_time', 'max'),
            pps_id = ('final_destination_location', 'first')
        )
        df = df.sort_values(by=['host', 'installation_id','ranger_type','ranger_version', 'ranger_id', 'time'], ascending=[True, True, True, True, True, True]).reset_index(drop=True)
        return df    

    def get_next_task_assignment_time(self, df):
        df['next_task_assigned_time'] = np.nan
        df['next_task_assigned_time'] = df['task_assigned_time'].shift(-1)
        valid_next_task_assigned_mask = ((df['host']==df['host'].shift(-1)) & (df['installation_id']==df['installation_id'].shift(-1)) & (df['ranger_id']==df['ranger_id'].shift(-1)) & (df['ranger_type']==df['ranger_type'].shift(-1)))
        df.loc[~valid_next_task_assigned_mask, 'next_task_assigned_time'] = np.nan
        return df
    
    def get_prev_incomplete_cycles(self):
        q = f"select * from relay_idle_time where time>'{self.back_date}' and time<'{self.end_date}' and (cycle_time=0 or idle_time=0) "
        df = pd.DataFrame(self.client.query(q).get_points()) 
        cols = ['time', 'task_key','subtask_key','host','installation_id','ranger_version','ranger_type','ranger_id','task_assigned_time','task_completed_time', 'next_task_assigned_time','cycle_time','idle_time']
        if df.empty:
            return pd.DataFrame(columns = cols)
        df = df[cols]
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df['task_assigned_time'] = pd.to_datetime(df['task_assigned_time'], utc=True)
        df['task_completed_time'] = pd.to_datetime(df['task_completed_time'], utc=True)
        df['next_task_assigned_time'] = pd.to_datetime(df['next_task_assigned_time'], utc=True)
        return df
    
    def calculate_time_diff(self, df):
        df['cycle_time'] = np.nan
        df['idle_time'] = np.nan
        df = self.CommonFunction.datediff_in_millisec(df, 'task_completed_time', 'task_assigned_time', 'cycle_time')
        df = self.CommonFunction.datediff_in_millisec(df, 'next_task_assigned_time', 'task_completed_time', 'idle_time')
        df = df[~pd.isna(df['task_assigned_time'])].reset_index(drop=True) 
        return df       

    def float_typecast(self, df, float_cols):
        for col in float_cols:
            df[col] = df[col].fillna(0).astype('float')
        return df
    
    def str_cols(self, df, str_cols):
        for col in str_cols:
            df[col] = df[col].fillna('').astype('str')
            df[col] = df[col].replace('NaT','')
        return df
    
    def typecast_cols(self, df):
        if 'event' in df.columns:
            del df['event']
        datetime_cols = ['task_assigned_time','task_completed_time','next_task_assigned_time']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                df[col] = df[col].apply(lambda x : x.strftime('%Y-%m-%dT%H:%M:%SZ') if not pd.isna(x) else '')
        float_cols = ['cycle_time', 'idle_time']
        str_cols = list(set(df.columns) - set(float_cols))
        df = self.float_typecast(df, float_cols)
        df = self.str_cols(df, str_cols)
        return df


    def write_data_to_influx(self, df):
        df['time'] = pd.to_datetime(df['time'])
        df = df.set_index('time')        
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "relay_idle_time", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['host','installation_id','ranger_id','ranger_type'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        print("Data Inserted!!")
        return None    

    def relay_idle_time_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"]) 
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        self.CommonFunction = CommonFunction()            

        check_start_date = self.client.get_start_date("relay_idle_time", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from ranger_events where time>'{check_start_date}' and time<='{check_end_date}' and (event = 'task_completed' or event = 'subtask_completed') limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.relay_idle_time_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("relay_idle_time", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("relay_idle_time", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.relay_idle_time_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e


    def relay_idle_time_final1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("relay_idle_time", self.tenant_info)
        self.back_date = pd.to_datetime(self.start_date) - timedelta(hours=24)
        self.back_date = self.back_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = end_date.replace(second=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date : {self.start_date}, back_date : {self.back_date}, end_date : {self.end_date}")

        df = self.fetch_raw_data()
        if not df.empty:
            df = self.create_cols(df)
            # df = self.group_data(df)
            # df = self.get_next_task_assignment_time(df)
            prev_df = self.get_prev_incomplete_cycles()
            df = pd.concat([df, prev_df], ignore_index=True)
            df = df.fillna('')
            df = self.group_data(df)
            df = self.get_next_task_assignment_time(df)
            df = self.calculate_time_diff( df)
            df = self.typecast_cols(df)
            self.write_data_to_influx(df)
            return None
        else:
            self.save_last_entry()
            return None            
    



with DAG(
        'relay_idle_time',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='relay_idle_time dag created',
        schedule_interval='*/15 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(seconds=3600),
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['relay_bot_journey'] == "Y" and (
                    tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                try:
                    final_task = PythonOperator(
                        task_id='relay_idle_time_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(RelayIdleTime().relay_idle_time_final,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=3600),
                    )
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='relay_idle_time_final',
            provide_context=True,
            python_callable=functools.partial(RelayIdleTime().relay_idle_time_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

