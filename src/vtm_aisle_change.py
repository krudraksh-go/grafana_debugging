## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
import numpy as np
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------
from config import (
    rp_seven_days
)
import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class VtmAisleChange:

    def vtm_aisle_change_final(self, tenant_info, **kwargs):
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
        check_start_date = self.client.get_start_date(f"{rp_seven_days}.vtm_aisle_change", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from ranger_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.vtm_aisle_change_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3(f"{rp_seven_days}.vtm_aisle_change", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3(f"{rp_seven_days}.vtm_aisle_change", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.vtm_aisle_change_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e
    
    def save_last_run(self):
        filter = {"site": self.tenant_info['Name'], "table": f"{rp_seven_days}.vtm_aisle_change"}
        new_last_run = {"last_run": self.end_date}
        self.CommonFunction.update_dag_last_run(filter, new_last_run)  
    
    def fill_aisle_id(self, df):
        df = df.replace('null', np.nan)
        df = df.sort_values(by=["task_key", "subtask_key", "time"], ascending=[True, True, True])
        df["aisle_id"] = (df.groupby(["task_key", "subtask_key"])["aisle_id"].ffill().bfill())  #ffill and bfill if task_key and subtask_key are same
        return df


    def fetch_raw_data(self):
        q = f"select * from ranger_events where time>'{self.start_date}' and time <= '{self.end_date}' and (action_type = 'loading' or action_type = 'unloading' ) and ranger_type = 'vtm' order by time"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        selected_cols = ['time','action_type','event','host','installation_id','ranger_id','ranger_type','task_key','task_status','aisle_id','subtask_key']
        if df.empty:
            return pd.DataFrame(columns = selected_cols)
        df = df.rename(columns = {'next_destination_aisle_id':'aisle_id'})
        df = self.fill_aisle_id(df)
        df = df[df['event'].isin(['loading_completed','unloading_completed'])].reset_index(drop=True)
        df = df[selected_cols]
        return df 

    def vtm_aisle_change_final1(self, end_date, **kwargs):
        self.original_start_date = self.client.get_start_date(f"{rp_seven_days}.vtm_aisle_change", self.tenant_info)
        self.start_date = pd.to_datetime(self.original_start_date) - timedelta(minutes=60)
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date : {self.start_date}, original_start_date : {self.original_start_date}, end_date : {self.end_date}")

        df = self.fetch_raw_data()
        if not df.empty:
            df = self.remove_duplicates(df)
            df = self.check_aisle_change(df)
            if df.empty:
                self.save_last_run()
                return None
            self.write_data(df)
        else:
            self.save_last_run()
            return None  

        return None

    def typecast_to_float(self, df, float_cols):
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(float)
        return df

    def typecast_to_str(self, df, str_cols):
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
        return df    
    
    def check_aisle_change(self, df):
        df = df.sort_values(by=['installation_id', 'ranger_type', 'ranger_id', 'time'], ascending=[True, True, True, True]).reset_index(drop=True)
        df['aisle_change'] = 0
        prev_cols = ['installation_id', 'ranger_type', 'ranger_id', 'aisle_id','task_key','subtask_key']
        for col in prev_cols:
            df[f'prev_{col}'] = df[col].shift(1)
            df[f'prev_{col}'] = df[f'prev_{col}'].fillna('**').astype(str)
        df['aisle_change'] = df.apply(lambda x : 1 if x['installation_id'] == x['prev_installation_id'] and x['ranger_type'] == x['prev_ranger_type'] and x['ranger_id'] == x['prev_ranger_id'] and x['aisle_id'] != x['prev_aisle_id'] and x['installation_id']!='**' else 0, axis=1)
        df['prev_time'] = np.nan
        df['prev_time'] = df['time'].shift(1)
        df['aisle_change_time'] = np.nan
        df = self.CommonFunction.datediff_in_millisec(df, 'time', 'prev_time', 'aisle_change_time')
        df = df[df['aisle_change'] == 1].reset_index(drop=True)
        for col in prev_cols:
            if col in df.columns and col not in ['aisle_id','task_key','subtask_key']:
                del df[f'prev_{col}']
        if 'prev_time' in df.columns:
            del df['prev_time']
        return df

    
    def remove_duplicates(self, df):
        df = df.sort_values(by=['installation_id', 'ranger_type', 'ranger_id', 'time'], ascending=[True, True, True, True]).reset_index(drop=True)
        df['del_flag'] = 0
        prev_cols = ['installation_id', 'ranger_type', 'ranger_id', 'event']
        for col in prev_cols:
            df[f'prev_{col}'] = df[col].shift(1)
        df['del_flag'] = df.apply(lambda x : 1 if x['installation_id'] == x['prev_installation_id'] and x['ranger_type'] == x['prev_ranger_type'] and x['ranger_id'] == x['prev_ranger_id'] and x['event'] == x['prev_event'] else 0, axis=1)
        df = df[df['del_flag'] == 0].reset_index(drop=True)
        for col in prev_cols:
            if col in df.columns:
                del df[f'prev_{col}']
        if 'del_flag' in df.columns:
            del df['del_flag']
        return df
    
    def write_data(self, df):
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')
        float_cols = ['aisle_change_time']
        df = self.typecast_to_float(df, float_cols)
        str_cols = [col for col in df if col not in float_cols]            
        df = self.typecast_to_str(df, str_cols)        
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "vtm_aisle_change", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['installation_id','host'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])    
        print("Inserted!!")    


with DAG(
        'vtm_aisle_change',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='vtm_aisle_change dag created',
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
            if tenant['Active'] == "Y" and tenant['relay_bot_journey'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='vtm_aisle_change_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(VtmAisleChange().vtm_aisle_change_final,
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
            task_id='vtm_aisle_change_final',
            provide_context=True,
            python_callable=functools.partial(VtmAisleChange().vtm_aisle_change_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

