import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, GCS
import os, json
from utils.CommonFunction import MongoDBManager
from config import (
    CONFFILES_DIR,
    GOOGLE_BUCKET_NAME,
    MongoDbServer
)

class tote_journey:
    def apply_group(self,df):
        group = 0
        for x in df.index:
            if x >= 0:
                if (df['event'][x] == 'loading_started' and df['current_location_type'][x]=='tote_storable') or (df['tote_id'][x]!=df['prev_tote_id'][x]) or (df['installation_id'][x]!=df['installation_id'][x-1]):
                    group = group + 1
                df['group'][x] = group
        return df
        
    
    def create_column(self, df, col):
        for i in col:
            df[i]=np.nan
        return df
    
    def find_time_diff(self, df, end_time, start_time, new_col):
        for index in range(len(start_time)):
            df = self.utilfunction.datediff_in_millisec(df, end_time[index],
                                                    start_time[index], new_col[index])
        return df
    
    def apply_group_events(self,final_df, combine_df, new_events, time_diff):
        for i in range(len(new_events)):
            final_df['new_event'] = new_events[i]
            final_df['time_diff'] = final_df[time_diff[i]]
            combine_df2 = final_df
            combine_df = pd.concat([combine_df,combine_df2])
        return combine_df

    def final_call(self,tenant_info,**kwargs):
        self.utilfunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("tote_journey", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from tote_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("tote_journey", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except Exception as e:
                print(f"error:{e}")

    def final_call1(self,end_date,**kwargs):
        self.start_date = self.client.get_start_date("tote_journey", self.tenant_info)
        self.utilfunction = CommonFunction()
        self.gcs = GCS()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')


        print(f"start_date:{self.start_date}\n end_date: {self.end_date}")
        path = f'butler_nav_incidents/{self.tenant_info["Name"]}_tote_cycle.csv'
        path = os.path.join(CONFFILES_DIR, path)
        source_blob_name = f'{self.tenant_info["Name"]}_tote_cycle.csv'
        self.gcs.download_from_bucket(GOOGLE_BUCKET_NAME, source_blob_name, path)
        isExist = os.path.isfile(path)

        # tote_events = pd.read_csv('/opt/airflow/dags/Tote Events - Sheet1.csv')
        q = f"select * from tote_events where time>='{self.start_date}' and time<='{self.end_date}' order by time"
        tote_events = pd.DataFrame(self.read_client.query(q).get_points())
        tote_events_col = tote_events.columns
        columns_to_keep = tote_events.columns
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        collection_name = source_blob_name.replace(".csv", '')
        self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
        data = self.cls_mdb.get_two_days_data(self.start_date)
        if data:
            cof = pd.DataFrame(data)
            cof['time'] = pd.to_datetime(cof['time'], unit='ms')
        elif isExist:
            cof = pd.read_csv(path)
        else:
            cof = pd.DataFrame(columns=columns_to_keep)

        cof = cof[cof.columns.intersection(columns_to_keep)]
        if tote_events.empty:
            filter = {"site": self.tenant_info['Name'], "table": "tote_journey"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return
        tote_events= pd.concat([tote_events,cof])
        tote_events = tote_events[(tote_events['event'] != 'reached_barcode')]
        tote_events['time'] = pd.to_datetime(tote_events['time'])
        tote_events = self.utilfunction.reset_index(tote_events)
        
        tote_events = tote_events.sort_values(by=['installation_id','tote_id','time'])
        tote_events = self.utilfunction.reset_index(tote_events)
        tote_events['group'] = 0
        tote_events['prev_location_type'] = tote_events['current_location_type'].shift(1).fillna('').astype(str)
        tote_events['prev_event'] = tote_events['event'].shift(1).fillna('').astype(str)
        tote_events['prev_tote_id'] = tote_events['tote_id'].shift(1).fillna(0).astype(str)
        tote_events['prev_time'] = tote_events['time'].shift(1)
        tote_events = self.utilfunction.reset_index(tote_events)
        tote_events = tote_events[(tote_events['event'] != tote_events['prev_event'])]
        tote_events = self.utilfunction.reset_index(tote_events)
        tote_events = self.apply_group(tote_events)
        tote_events['prev_location_type'] = tote_events['current_location_type'].shift(1).fillna('').astype(str)
        tote_events['prev_event'] = tote_events['event'].shift(1).fillna('').astype(str)
        tote_events['prev_tote_id'] = tote_events['tote_id'].shift(1).fillna(0).astype(str)
        tote_events['prev_time'] = tote_events['time'].shift(1)
        tote_events = self.utilfunction.reset_index(tote_events)
        tote_events['time_diff'] = np.nan
        tote_events = self.utilfunction.datediff_in_millisec(tote_events, 'time', 'prev_time', 'time_diff')
        tote_events['time_diff'] = tote_events.apply(
            lambda x: x['time_diff'] if x['tote_id'] == x['prev_tote_id'] and not pd.isna(x['time_diff']) else 0,
            axis=1)
        
        col = ['loaded_storable','check_safe_to_drop','unloading_started_conveyor','unloaded_conveyor','notified_conveyor_going_pps', 'going_to_pps_point', 'reached_pps_point','notified_conveyor_leavin_pps','reached_conveyor_exit','loading_started_conveyor', 'loaded_conveyor', 'unloading_started_storable','unloaded_storable','tote_deletion']
        tote_events = self.create_column(tote_events,col)

        tote_events['loaded_storable'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='loaded' and x['destination_type']=='pps' else np.nan, axis=1)
        tote_events['check_safe_to_drop'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='check_safe_to_drop' else np.nan, axis=1)
        tote_events['unloading_started_conveyor'] = tote_events.apply(
                lambda x: x['time'] if x['event']=='unloading_started' and x['destination_type']=='pps' else np.nan, axis=1)
        tote_events['unloaded_conveyor'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='unloaded' and x['current_location_type']=='conveyor_entry' else np.nan, axis=1)
        tote_events['notified_conveyor_going_pps'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='notified_conveyor' and x['current_location_type']=='conveyor_entry' else np.nan, axis=1)
        tote_events['going_to_pps_point'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='going_to_pps_point' else np.nan, axis=1)
        tote_events['reached_pps_point'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='reached_pps_point' else np.nan, axis=1)
        tote_events['notified_conveyor_leaving_pps'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='notified_conveyor' and x['current_location_type']=='conveyor_pps_point' else np.nan, axis=1)
        tote_events['cycle_start'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='loading_started' and x['current_location_type']=='tote_storable' else np.nan, axis=1)
        tote_events['cycle_ends'] = tote_events.apply(
                    lambda x: x['time'] if (x['event']=='unloaded' and x['current_location_type']=='tote_storable') or (x['event']=='tote_deletion') else np.nan, axis=1)
        tote_events['reached_conveyor_exit'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='reached_conveyor_exit' else np.nan, axis=1)
        tote_events['loading_started_conveyor'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='loading_started' and x['current_location_type']=='conveyor_exit' else np.nan, axis=1)
        tote_events['loaded_conveyor'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='loaded' and x['destination_type']=='tote_storable' else np.nan, axis=1)
        tote_events['unloading_started_storable'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='unloading_started' and x['destination_type']=='tote_storable' else np.nan, axis=1)
        tote_events['unloaded_storable'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='unloaded' and x['current_location_type']=='tote_storable' else np.nan, axis=1)
        tote_events['tote_deletion'] = tote_events.apply(
                    lambda x: x['time'] if x['event']=='tote_deletion' else np.nan, axis=1)
        

        tote_events = self.utilfunction.reset_index(tote_events)

        
        group_df = tote_events.groupby(['installation_id','tote_id', 'group'], as_index=False).agg(
            loaded_storable_time = ('loaded_storable','min'),
            first_check_safe_to_drop_time = ('check_safe_to_drop','min'),
            unloading_started_conveyor_time = ('unloading_started_conveyor','min'),
            unloaded_conveyor_time = ('unloaded_conveyor','max'),
            notified_conveyor_going_pps_time = ('notified_conveyor_going_pps','min'),
            going_to_pps_point_time = ('going_to_pps_point','min'),
            reached_pps_point_time = ('reached_pps_point','min'),
            notified_conveyor_leaving_pps_time = ('notified_conveyor_leaving_pps','min'),
            reached_conveyor_exit_time = ('reached_conveyor_exit','min'),
            loading_started_conveyor_time = ('loading_started_conveyor','min'),
            loaded_conveyor_time = ('loaded_conveyor','max'),
            unloading_started_storable_time = ('unloading_started_storable','min'),
            unloaded_storable_time = ('unloaded_storable','max'),
            cycle_start_time = ('cycle_start','min'),
            cycle_end_time = ('cycle_ends','max'),
            tote_deletion_time = ('tote_deletion','max')
            )
       
        group_df = group_df[group_df['cycle_start_time'].notnull()]
        group_df = self.utilfunction.reset_index(group_df)
        tote_events = pd.merge(tote_events, group_df, how='left', on=['installation_id','tote_id', 'group'])
        incomplete_cycle = tote_events[(~pd.isna(tote_events['cycle_start_time'])) & (pd.isna(tote_events['cycle_end_time']))]
        tote_events = tote_events[(~pd.isna(tote_events['cycle_start_time'])) & (~pd.isna(tote_events['cycle_end_time']))]
        tote_events = self.utilfunction.reset_index(tote_events)
        incomplete_cycle = self.utilfunction.reset_index(incomplete_cycle)

        if tote_events.empty:
            if not incomplete_cycle.empty:
                if self.cls_mdb.delete_data({}, multiple=True):
                    json_list = incomplete_cycle.to_json(orient="records")
                    json_list = json.loads(json_list)
                    test = self.cls_mdb.insert_data(json_list)
                else:
                    incomplete_cycle.to_csv(path, index=False, header=True)
                    self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
                    #print(test)
                # incomplete_cycle.to_csv(path, index=False, header=True)
                # self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
            filter = {"site": self.tenant_info['Name'], "table": "tote_journey"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return

        tote_events['new_event'] = np.nan

        col = ['conveyor_entry_to_pps1','ppsN_to_exit','tote_load_to_stored','tote_load_to_conveyor','entry_wait_time','exit_wait_time','incoming_ranger_time','outgoing_ranger_time','conveyor_entry_time','conveyor_pps_point_time','conveyor_exit_time','time_to_delete_tote']
        tote_events = self.create_column(tote_events,col)

        start_time = ['unloaded_conveyor_time', 'notified_conveyor_leaving_pps_time','loaded_conveyor_time',            'loaded_storable_time' ,             'notified_conveyor_going_pps_time', 'reached_conveyor_exit_time',       'loaded_storable_time',            'loaded_conveyor_time',             'unloaded_conveyor_time',  'reached_pps_point_time',               'reached_conveyor_exit_time'   ,  'notified_conveyor_leaving_pps_time' ]
        end_time =   ['reached_pps_point_time', 'reached_conveyor_exit_time',        'unloading_started_storable_time', 'unloading_started_conveyor_time',   'going_to_pps_point_time'  ,        'loading_started_conveyor_time',    'unloading_started_conveyor_time',  'unloading_started_storable_time', 'going_to_pps_point_time',  'notified_conveyor_leaving_pps_time',  'loading_started_conveyor_time',  'tote_deletion_time' ]
        new_col =    ['conveyor_entry_to_pps1', 'ppsN_to_exit',                      'tote_load_to_stored',             'tote_load_to_conveyor',             'entry_wait_time',                  'exit_wait_time',                  'incoming_ranger_time',             'outgoing_ranger_time',            'conveyor_entry_time',       'conveyor_pps_point_time',             'conveyor_exit_time',             'time_to_delete_tote' ] 

        tote_events = self.find_time_diff(tote_events, end_time,start_time,new_col)
        tote_events['prev_group'] = tote_events['group'].shift(1).fillna(-1).astype(int)

        tote_events['new_event'] = tote_events.apply(
            lambda x: 'Misc_' + str(x['event']) + '_' + str(x['prev_event']) if x['group']==x['prev_group'] else 'different group', axis=1)        

        
        final_df = tote_events[tote_events['event']=='loading_started']
        final_df = final_df[tote_events['current_location_type']=='tote_storable']
        final_df = self.utilfunction.reset_index(final_df)
        combine_df = pd.DataFrame(columns=final_df.columns)
        new_event = ['conveyor entry to pps','pps to exit','tote load to stored','tote load to conveyor','entry wait time','exit wait time', 'tote on bot going to conveyor', 'tote on bot going to storable', 'tote on conveyor entry', 'tote on pps point', 'tote on conveyor exit','time to delete tote']
        time_diff = ['conveyor_entry_to_pps1','ppsN_to_exit','tote_load_to_stored','tote_load_to_conveyor','entry_wait_time','exit_wait_time','incoming_ranger_time','outgoing_ranger_time','conveyor_entry_time','conveyor_pps_point_time','conveyor_exit_time','time_to_delete_tote']
        combine_df = self.apply_group_events(final_df, combine_df, new_event,time_diff)
        combine_df = self.utilfunction.reset_index(combine_df)
        for x in combine_df.index:
            combine_df['time'][x] = pd.to_datetime(combine_df['time'][x]) + timedelta(milliseconds=x)

        tote_events = pd.concat([tote_events, combine_df])
        tote_events = self.utilfunction.reset_index(tote_events)
        tote_events = tote_events[tote_events['time_diff'].notnull()]
        tote_events = tote_events[['time','tote_id','new_event','time_diff','event_reason','ranger_tray_id','current_location','installation_id', 'destination', 'destination_type']]
        tote_events.rename(columns={'new_event': 'event', }, inplace=True)
        tote_events = self.utilfunction.reset_index(tote_events)  
        tote_events = tote_events.set_index('time')  
        if 'index' in tote_events.columns:
            del tote_events['index']

        tote_events['tote_id'] = tote_events['tote_id'].astype(str,errors='ignore')
        tote_events['event'] = tote_events['event'].astype(str,errors='ignore')
        tote_events['time_diff'] = tote_events['time_diff'].astype(int,errors='ignore')
        tote_events['destination'] = tote_events['destination'].astype(str,errors='ignore')
        tote_events['destination_type'] = tote_events['destination_type'].astype(str,errors='ignore')

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                            port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(tote_events, "tote_journey", db_name=self.tenant_info["alteryx_out_db_name"],
                                    tag_columns=['event'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        if not incomplete_cycle.empty:
            if self.cls_mdb.delete_data({}, multiple=True):
                json_list = incomplete_cycle.to_json(orient="records")
                json_list = json.loads(json_list)
                test = self.cls_mdb.insert_data(json_list)
        else:
            incomplete_cycle.to_csv(path, index=False, header=True)
            self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
                        #print(test)
                # incomplete_cycle.to_csv(path, index=False, header=True)
                # self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
        
# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'Tote_Journey',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Tote Journey',
    schedule_interval = '39 * * * *',
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
            if tenant['Active'] == "Y" and tenant['tote_journey'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='tote_journey_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(tote_journey().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='tote_journey_final',
            provide_context=True,
            python_callable=functools.partial(tote_journey().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )