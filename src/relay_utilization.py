import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
import numpy as np

class RelayUtilization:

    def relay_utilization_final(self, tenant_info, **kwargs):
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
        check_start_date = self.client.get_start_date("relay_utilization", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from ranger_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.relay_utilization_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("relay_utilization", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.relay_utilization_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e
    
    
    def fetch_data(self):
        q = f"select * from ranger_events where time>'{self.start_date}' and time <= '{self.end_date}' and (event = 'action_finished' or event = 'action_started') and (action_type = 'loading' or action_type = 'unloading') and (task_status = 'tote_picked' or task_status='storing' or task_status = 'loading_from_relay' or task_status = 'unloading_at_relay' or task_status = 'assigned') "
        # print(q)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        cols = ['time','event','action_type','current_coordinate','current_location','entity_id','host','installation_id','task_status','aisle_id','ranger_type']
        if df.empty:
            return pd.DataFrame(columns=cols)
        df = df.rename(columns={'next_destination_aisle_id':'aisle_id'})
        df = df.fillna('')
        df = df.sort_values(by=['installation_id','task_key','subtask_key','time'])
        df['prev_location'] = df['current_location'].shift(1)
        df['can_shift'] = ((df['installation_id']==df['installation_id'].shift(1)) & (df['task_key']==df['task_key'].shift(1)) & (df['subtask_key'] == df['subtask_key'].shift(1))  & (df['event']=='action_finished') & ((df['current_location']=='') | (df['current_location']=='null') ) )
        df['current_location'] = df.apply(lambda x : x['prev_location'] if x['can_shift'] else x['current_location'], axis = 1)
        df['event'] = df.apply(lambda x : 'loading' if x['action_type']=='loading' and x['event']=='action_finished' else x['event'], axis = 1)
        df['event'] = df.apply(lambda x : 'unloading' if x['action_type']=='unloading' and x['event']=='action_finished' else x['event'], axis = 1)
        df = df[cols]
        df = df[df['event'].isin(['loading','unloading'])].reset_index(drop=True)
        df.time = pd.to_datetime(df.time)
        return df
    
    def save_last_run(self):
        filter = {"site": self.tenant_info['Name'], "table": "relay_utilization"}
        new_last_run = {"last_run": self.end_date}
        self.CommonFunction.update_dag_last_run(filter, new_last_run)
        return None
    
    def write_data_to_influx(self, df):
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "relay_utilization", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['aisle_id', 'current_location','state'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        print("Inserted!!")
        return None
    
    def typecast_cols(self, df):
        df['time'] = df['last_repoting_time']
        df = df.set_index('time')
        final_cols = ['host','installation_id','aisle_id','current_location','state','state_start_time','last_repoting_time','dwell_time','time_diff','state_end','entity_id','ranger_type','task_status']
        df = df[final_cols]
        float_cols = ['time_diff','dwell_time']
        for col in float_cols:
            df[col] = df[col].fillna(0).astype(float)
        str_cols = set(df.columns) - set(float_cols)
        for col in str_cols:
            df[col] = df[col].fillna('').astype(str)
        return df

    def rename_events(self, df):
        # event,  new_event
        new_event_map = [
            ['unloading','occupied'],
            ['loading','vacant'],
            ['unloading','occupied'],
            ['loading','vacant']
        ]
        cols = ['event','state']
        new_event_df = pd.DataFrame(new_event_map, columns=cols)
        df = pd.merge(df, new_event_df, on=['event'], how='inner')
        return df

    def remove_duplicates(self, df):
        df = df.sort_values(by=['host','installation_id','current_location','time'])
        df =df[~((df['installation_id']==df['installation_id'].shift(1)) & (df['host']==df['host'].shift(1)) & (df['current_location']==df['current_location'].shift(1)) & (df['state']==df['state'].shift(1)))].reset_index(drop=True)
        return df
    
    def fetch_inprogress_state(self):
        q = f"select * from relay_utilization where time>'{self.back_date}' and time <= '{self.start_date}' and state_end ='False' "
        df = pd.DataFrame(self.client.query(q).get_points())
        if df.empty:
            return pd.DataFrame(columns = ['time','host','installation_id','current_location','state','entity_id','aisle_id','ranger_type','task_status'])
        df = df.groupby(['host','installation_id','current_location']).agg({'time':'last','state_start_time': 'last','state':'last','entity_id':'last','aisle_id':'last', 'ranger_type':'last','task_status':'last'}).reset_index()
        return df
    
    def cal_state_start_time(self, df):
        if 'state_start_time' not in df.columns:
            df['state_start_time'] = np.nan
        df['state_start_time'] = df.apply(lambda x : x['time'] if pd.isna(x['state_start_time']) else x['state_start_time'], axis = 1)
        return df   

    def update_location_without_state(self, df):
        df['time_diff'] = np.nan
        df['time'] = pd.to_datetime(self.end_date, utc=True)
        df['start_date'] = pd.to_datetime(self.start_date, utc=True)
        df = self.CommonFunction.datediff_in_millisec(df, 'last_repoting_time', 'start_date', 'time_diff')
        df['old'] = 0
        return df
     
    def cal_last_reporting_time(self, df):
        df['last_repoting_time'] = df['time'].shift(-1)
        df['last_repoting_time'] = df.apply(lambda x : pd.to_datetime(self.end_date, utc=True) if x['state_end']==False else x['last_repoting_time'], axis = 1)
        return df
    
    def check_state_ends(self, df):
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df = df.sort_values(by=['host','installation_id','current_location','time'], ascending=[True,True,True,True]).reset_index(drop=True)
        df['state_end'] = ((df['installation_id']==df['installation_id'].shift(-1)) & (df['host']==df['host'].shift(-1)) & (df['current_location']==df['current_location'].shift(-1)))
        return df
    
    def calculate_time_differences(self, df):
        df['time_diff'] = np.nan
        df['dwell_time'] = np.nan
        df = self.CommonFunction.datediff_in_millisec(df, 'last_repoting_time', 'time', 'time_diff')
        df = self.CommonFunction.datediff_in_millisec(df, 'last_repoting_time', 'state_start_time', 'dwell_time')
        return df
    
    def add_aisle_id(self, df):
        df = df.sort_values(by=['installation_id','host','current_location','time'])
        df['aisle_id'] = df['aisle_id'].replace('',np.nan).replace('null',np.nan)
        df['can_shift_aisle'] = ((df['installation_id']==df['installation_id'].shift(1)) & (df['host']==df['host'].shift(1)) & (df['current_location']==df['current_location'].shift(1)) & (pd.isna(df['aisle_id'])) )
        df['new_aisle_id'] = df['aisle_id']
        df['new_aisle_id'] = df['new_aisle_id'].ffill()
        df['aisle_id'] = df.apply(lambda x : x['new_aisle_id'] if x['can_shift_aisle']==True else x['aisle_id'] , axis = 1)
        return df

    def relay_utilization_final1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("relay_utilization", self.tenant_info)
        self.back_date = (pd.to_datetime(self.start_date) - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    
        df = self.fetch_data()
        if not df.empty:
            df = self.rename_events(df)
            df = self.remove_duplicates(df)
            prev_df = self.fetch_inprogress_state()
            df = pd.concat([df, prev_df], ignore_index=True)
            df = self.check_state_ends(df)
            df = self.add_aisle_id(df)
            df = self.cal_state_start_time(df)
            df = self.cal_last_reporting_time(df)
            df = self.calculate_time_differences(df)
            df = self.typecast_cols(df) 
            self.write_data_to_influx(df)        
        else:
            self.save_last_run()
            return None        
        return None


with DAG(
        'relay_utilization',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='relay_utilization dag created',
        schedule_interval='15 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(seconds=1200),
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
                        task_id='relay_utilization_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(RelayUtilization().relay_utilization_final,
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
            task_id='relay_utilization_final',
            provide_context=True,
            python_callable=functools.partial(RelayUtilization().relay_utilization_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

