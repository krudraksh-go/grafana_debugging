import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, GCS
import os
from config import (
    CONFFILES_DIR,
    GOOGLE_BUCKET_NAME
    )
class Bot_To_Bot_Time:

    def delete_prev_columns(self, df,cols):
        for col in cols:
            new_col = 'prev_'+col
            if new_col in df.columns:
                del df[new_col]
        return df

    def create_prev_event(self, df, cols):
        for col in cols:
            new_col = 'prev_'+col
            df[new_col] = np.nan
            df[new_col] = df[col].shift(1)
        return df

    def on_same_location(self, row):
        if row['installation_id']==row['prev_installation_id'] and row['journey_destination']==row['prev_journey_destination'] and row['current_location']==row['prev_current_location']:
            return True
        return False
    
    def cal_time_difference(self, row):
        if self.on_same_location(row):
            if row['event']=='going_to_destination' and row['prev_event']=='reached_destination':
                time_on_destination = (row['time']-row['prev_time']).total_seconds()
                return time_on_destination
            elif row['event']=='reached_destination' and row['prev_event']=='going_to_destination':
                bot_to_bot_time = (row['time']-row['prev_time']).total_seconds()
                return bot_to_bot_time
        return 0

    def final_call(self,tenant_info,**kwargs):
        self.CommonFunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.site = self.tenant_info['Name']

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("bot_arrival_time",self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")
        q = f"select *  from ranger_task_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("bot_arrival_time", '15min',self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
        
    def final_call1(self,end_date,**kwargs):
        self.original_start_date = self.client.get_start_date("bot_arrival_time",self.tenant_info)
        self.start_date = pd.to_datetime(self.original_start_date) - timedelta(hours=1)
        self.CommonFunction = CommonFunction()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}, original_start_date:{self.original_start_date}")

        q = f""" select * from ranger_task_events where time >='{self.start_date}' and time<='{self.end_date}' 
        and (current_location_type = 'port_exit_io_point' or current_location_type = 'port_entry_io_point' or current_location_type = 'conveyor_entry_io_point' or current_location_type = 'conveyor_exit_io_point' ) 
        and (event = 'going_to_waiting_point' or event = 'going_to_queue_point' or event = 'going_to_destination' or event='reached_destination') """
        
        ranger_task_events = pd.DataFrame(self.read_client.query(q).get_points())
        if ranger_task_events.empty:
            filter = {"site": self.tenant_info['Name'], "table": "bot_arrival_time"} 
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter,new_last_run)  
            return None

        ranger_task_events['current_location_type'] = ranger_task_events['current_location_type'].apply(lambda x: 'entry' if ('ENTRY' in x.upper() ) else ('exit' if ('EXIT' in x.upper()) else 'none'))
        df = ranger_task_events[['time','current_location','current_location_type','event','installation_id','journey_destination','journey_type','ranger_id']]
        df['time'] = pd.to_datetime(df['time'],utc=True)
        df = df.sort_values(by=['installation_id','journey_destination', 'current_location_type', 'time'],ascending=[True, True,True, True]).reset_index(drop=True)
        prev_cols = ['installation_id','event','time','installation_id','journey_destination','current_location']
        df = self.create_prev_event(df,prev_cols)
        df['value'] = df.apply(lambda x: self.cal_time_difference(x), axis=1)
        df = self.delete_prev_columns(df,prev_cols)
        df = df[df['value']>0].reset_index(drop=True)
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "bot_arrival_time"} 
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter,new_last_run)  
            return None
        
        df['event'] = df['event'].apply(lambda x: 'Time on Destination' if x=='going_to_destination' else ( 'Bot to Bot Time' if x=='reached_destination' else 'Undefined'))
        
        df = df.set_index('time')
        str_cols = ['current_location','current_location_type', 'event','installation_id','journey_destination','journey_type','ranger_id']
        float_cols = ['value']
        df[float_cols] = df[float_cols].fillna(0)
        df = self.CommonFunction.str_typecast(df,str_cols)
        df = self.CommonFunction.float_typecast(df,float_cols)
        print("inserted")

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "bot_arrival_time", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['journey_destination'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None