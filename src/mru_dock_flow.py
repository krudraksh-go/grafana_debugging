import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, MongoDBManager
import os
from config import (
    MongoDbServer
    )
import json
import time
class MruDockFlow:

    def connect_to_mongodb(self):
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        collection_name = self.tenant_info['Name']+'_mru_dock_flow'
        self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)

    def save_incomplete_cycle(self, df):
        df['time'] = df['time'].apply(lambda x: self.utilfunction.convert_to_nanosec(x))
        if not df.empty:
            test = self.cls_mdb.delete_data({}, multiple=True)
            json_list = df.to_json(orient="records")
            json_list = json.loads(json_list)
            self.cls_mdb.insert_data(json_list)
    
    def get_incomplete_cycle(self):
        data = self.cls_mdb.get_two_days_data(self.start_date)
        df = pd.DataFrame(data)
        return df

    def apply_group(self, df):
        Group=1    
        for x in df.index:
            if x > 0:
                if df['installation_id'][x]==df['installation_id'][x-1] and df['pps_id'][x]==df['pps_id'][x-1] and df['rack_id'][x]==df['rack_id'][x-1] and df['event'][x]!='dock_complete' :
                    df["group"][x] = df["group"][x-1]
                else:
                    Group=Group+1
                    df["group"][x] = Group
            else:
                df["group"][x] = Group
        return df
    
    def final_call(self,tenant_info,**kwargs):
        self.CommonFunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("mru_dock_flow", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select * from mru_flow_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("mru_dock_flow", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e


    def final_call1(self,end_date,**kwargs):
        self.start_date = self.client.get_start_date("mru_dock_flow", self.tenant_info)
        self.utilfunction = CommonFunction()
        self.end_date = end_date
        self.start_date = pd.to_datetime(self.start_date).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.two_days_before = (pd.to_datetime(self.start_date) - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}")

        q = f"select * from mru_flow_events where time>='{self.start_date}' and time<='{self.end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "mru_dock_flow"}
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)
            return     
        else:
            q = f"select * from mru_flow_events where time>='{self.start_date}' and time<='{self.end_date}'"
            df = pd.DataFrame(self.read_client.query(q).get_points())
            initial_cols = df.columns
            self.connect_to_mongodb()
            prev_incomplete_cycles = self.get_incomplete_cycle()
            self.cls_mdb.close_connection()
            if prev_incomplete_cycles.empty:
                prev_incomplete_cycles = pd.DataFrame(columns=initial_cols)
                prev_incomplete_cycles=prev_incomplete_cycles.drop_duplicates(subset=['installation_id','pps_id','group_id','event'], keep='last')
            df = pd.concat([df,prev_incomplete_cycles]).reset_index(drop=True)

            df['time']=pd.to_datetime(df['time'],utc=True)
            # initial_cols = df.columns
            df = df.fillna('')
            df['group'] = 0
            df['rack_id'] = df.apply(lambda x: x['rollcage_id'] if (x['rack_id'] == '' or pd.isna(x['rack_id']) ) else x['rack_id'], axis = 1)
            # mru_flow_events = self.apply_group(df)
            df = df.sort_values(by=['installation_id', 'rack_id', 'time'], ascending=[True, True, True]).reset_index(drop=True)
            # df = df.sort_values(by=['installation_id', 'rack_id', 'time'], ascending=[True, True, True]).reset_index(drop=True)
            df = self.apply_group(df)
            mru_flow_events = df.copy()

            dock_station_df = df[df['event']=='dock_complete'][['installation_id','dock_station_id','group']].reset_index(drop=True)
            dock_station_df=dock_station_df.drop_duplicates( keep='last')
            dock_station_df = dock_station_df.rename(columns = {'dock_station_id':'new_dock_station_id'})
            df = pd.merge(df,dock_station_df, on = ['installation_id','group'], how = 'left')
            if 'dock_station_id' in df.columns:
                del df['dock_station_id']
            df = df.rename(columns = {'new_dock_station_id':'dock_station_id'})
            df = df[~pd.isna(df['dock_station_id'])]
            df['dock_station_id'] = df['dock_station_id'].astype(int)
            df = df.sort_values(by=['installation_id', 'dock_station_id', 'time'], ascending=[True, True, True]).reset_index(drop=True)

            df['prev_dock_station_id'] = df['dock_station_id'].shift(1)
            df['next_dock_station_id'] = df['dock_station_id'].shift(-1)
            df['prev_event'] = df['event'].shift(1)
            df['next_event'] = df['event'].shift(-1)
            df['prev_time'] = df['time'].shift(1)
            df = df[~pd.isna(df['prev_event'])].reset_index(drop=True)
            if df.empty:
                return
            df['cycle'] = 0
            df['cycle'] = df.apply(lambda x: 1 if (x['event'] == 'undock_complete' and x['next_event'] == 'dock_complete' and x['dock_station_id'] == x['next_dock_station_id'] ) else x['cycle'], axis = 1)
            cycle_status = df[df['event']=='undock_complete'][['group','cycle']]
            if 'cycle' in df.columns:
                del df['cycle']
            cycle_status = cycle_status.drop_duplicates(keep='last')
            df = pd.merge(df,cycle_status, on = ['group'], how = 'left')
            df['cycle'] = df['cycle'].fillna(0)
            df['cycle'] = df['cycle'].astype(int)
            incomplete_cycles = pd.merge(cycle_status[cycle_status['cycle']==0], mru_flow_events, on = ['group'], how = 'inner')
            incomplete_cycles = incomplete_cycles[initial_cols]

            df['event_name'] = ''
            df['event_name'] = df.apply(lambda x: x['prev_event']+'_to_'+x['event'], axis = 1)
            df['time_diff'] = np.nan
            df['time_diff'] = df.apply(lambda x: (x['time']-x['prev_time']).total_seconds() if x['dock_station_id']==x['prev_dock_station_id'] else np.nan,axis=1)
            df = df[~pd.isna(df['time_diff'])].reset_index(drop=True)
            df['time_diff'] = df['time_diff'].astype(int)
            df['rack_face'] = df['rack_face'].astype(str)
            df['dock_station_id'] = df['dock_station_id'].astype(str)
            cols_to_keep = ['time', 'dock_station_id', 'event', 'fill_percent_float',
                            'filled_slots_int', 'fulfilment_area', 'group_id', 'host',
                            'installation_id', 'pps_id', 'rack_face', 'rack_id', 'rack_type',
                            'rollcage_id', 'rollcage_type', 'task_id', 'total_slots_int',
                            'undock_reason', 'value', 'event_name','time_diff']
            df = df[cols_to_keep]
            df = df.set_index('time')

            if not incomplete_cycles.empty:
                self.connect_to_mongodb()
                self.save_incomplete_cycle(incomplete_cycles)
                self.cls_mdb.close_connection()
                
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                    port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df, "mru_dock_flow", db_name=self.tenant_info["alteryx_out_db_name"],
                                            tag_columns=['dock_station_id','event_name'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            print("inserted")
        return None
# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'mru_dock_flow',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'MRU flow Dock Undock time diff',
    schedule_interval = '59 * * * *',
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
            if tenant['Active'] == "Y" and tenant['mru_dock_flow'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='mru_dock_flow_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(MruDockFlow().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='mru_dock_flow_final',
            provide_context=True,
            python_callable=functools.partial(MruDockFlow().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )