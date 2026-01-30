import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, GCS
import os, json
from utils.CommonFunction import MongoDBManager
from bot_to_bot_time_calculation import Bot_To_Bot_Time
from config import (
    CONFFILES_DIR,
    GOOGLE_BUCKET_NAME,
    MongoDbServer
    )
class ttp_bot_journey:

    def clean_journey_destination_type_cols(self, df):
        df[['journey_destination','journey_type']] = df[['journey_destination','journey_type']].astype(str, errors = 'ignore')
        df['journey_destination'] = df['journey_destination'].apply(lambda x: '' if (pd.isna(x) or x=='null' or x=='nan' or x=='None') else x.rstrip('.0'))
        df['journey_type'] = df['journey_type'].apply(lambda x: '' if (pd.isna(x) or x=='null' or x=='nan' or x=='None') else x)
        return df

    def find_journey_variant(self, reached_queue_point,reached_waiting_point):
        via_queue = not pd.isna(reached_queue_point)
        via_wait = not pd.isna(reached_waiting_point)
        if via_queue and via_wait:
            return 'both wait and queue point'
        elif via_queue and not via_wait:
            return 'queue point'
        elif not via_queue and via_wait:
            return 'wait point'
        else:
            return 'direct'

    def find_time_diff(self, df, filter_event, new_event, first_event, second_event):
        fil = (df['event'] == filter_event)
        temp = df[fil]
        if temp.empty:
            return pd.DataFrame(columns = ['time','installation_id','event','curr_event','prev_event', 'event_reason', 'time_diff','current_location','current_location_type','next_destination','next_destination_type','host', 'ranger_id', 'load_on_ranger', 'task_key', 'task_status', 'task_type', 'journey','wait_point','journey_destination_type','journey_variant', 'journey_old'])
        
        temp = temp.groupby('task_key').last().reset_index()
        res = self.utilfunction.datediff_in_millisec(temp, second_event,first_event, 'time_diff')
        res['event'] = new_event
        if 'index' in res.columns:
            del res['index']
        return res

    def time_to_reach_destination_ealy_dispactch(self, df):

        df['curr_event'] = df['event']
        df['time_diff'] = np.nan
        
        col = ['time','installation_id','event','curr_event','prev_event', 'event_reason', 'time_diff','current_location','current_location_type','next_destination','next_destination_type','host', 'ranger_id', 'load_on_ranger', 'task_key', 'task_status', 'task_type', 'journey','wait_point','journey_destination_type','journey_variant', 'journey_old']
        final = pd.DataFrame(columns=col)

        temp = self.find_time_diff(df,'reached_destination','task_assign_to_reached_dest','task_assignment_time','reached_destination_early_dispatch_time')
        final = pd.concat([final,temp[col]])

        temp = self.find_time_diff(df,'reached_destination','waiting_point_to_reached_destination','going_to_destination_time','reached_destination_early_dispatch_time')
        final = pd.concat([final,temp[col]])
        
        temp = self.find_time_diff(df,'reached_waiting_point','task_assignment_to_reached_waiting_point','task_assignment_time','reached_waiting_point_time')
        final = pd.concat([final,temp[col]])

        temp = self.find_time_diff(df,'going_to_destination','reached_wait_point_to_going_to_destination','reached_waiting_point_time','going_to_destination_time')
        final = pd.concat([final,temp[col]])

        temp = self.find_time_diff(df,'reached_queue_point','going_to_queue_point_to_reached_queue_point','going_to_queue_point_time','reached_queue_point_time')
        final = pd.concat([final,temp[col]])

        temp = self.find_time_diff(df,'reached_queue_point','time_at_queue_point','reached_queue_point_time','going_to_destination_time')
        final = pd.concat([final,temp[col]])

        temp = self.find_time_diff(df,'reached_waiting_point','time_at_wait_point','reached_waiting_point','going_to_destination_time')
        final = pd.concat([final,temp[col]])

        temp = self.find_time_diff(df,'reached_waiting_point','time_at_wait_point','reached_waiting_point','going_to_queue_point_time')
        final = pd.concat([final,temp[col]])

        final['time_diff'] = final['time_diff'].apply(lambda x: 0 if (x=='' or pd.isna(x) or x==np.nan) else x)
        final['time_diff'] = final['time_diff'].astype(float, errors='ignore')
        final = final[final['time_diff']>0]

        final['wait_point'] = final['wait_point'].apply(lambda x: 'yes' if x==1 else 'no')

        final['time_ns'] = final['time'].apply(lambda x: x.timestamp())
        final['time_ns'] = final['time_ns']*1e9

        final['journey_old'] = final.apply(lambda x:'port' if x['journey_old']==1 else ('conveyor' if x['journey_old']==0 else 'No Port No Conveyor') ,axis=1)
        final['journey'] = final.apply(lambda x: x['journey_old'] if (pd.isna(x['journey']) or x['journey']=='' ) else x['journey'], axis = 1)

        if 'journey_old' in final.columns:
            del final['journey_old']

        final = self.utilfunction.reset_index(final)
        if 'level_0' in final.columns:
            del final['level_0']
        if 'index' in final.columns:
            del final['index']
        final = final.set_index('time')

        return final

    def cal_time_to_reach_location_where_tote_loaded(self,df):
        fil = (df['event']=='Time to reach storable')
        df1 = df[fil]
        df1['tote_order_count'] = df1['tote_order_count'].replace(0, np.nan)
        df1['tote_order_count'] = df1['tote_order_count'].fillna(method='ffill')
        group_df = df1.groupby(['installation_id','ranger_id', 'task_key','tote_order_count'], as_index=False).agg(
            tote_order_count = ('tote_order_count','last'),
            time_diff = ('time_diff','sum'))
        group_df['tote_order_count'] = group_df['tote_order_count'].astype(int,errors='ignore')
        group_df['group_event'] = 'Time to reach storable where tote loaded'

        group_df2 = df1.groupby(['installation_id','ranger_id', 'task_key'], as_index=False).agg(
            tote_order_count = ('event','count'),
            time_diff = ('time_diff','sum'))

        group_df2['group_event'] = 'total time spend in storable moving'
        return group_df, group_df2
    
    def cal_consolidated_info(self,df, filter, event_name, groupby_list):
        fil = (df['event']==filter)
        df1 = df[fil]
        group_df = df1.groupby(groupby_list,as_index=False).agg(
            tote_order_count = ('event','count'),
            time_diff = ('time_diff','sum'))
        group_df['group_event'] = event_name
        return group_df
    
    def get_distinct_aisle(self, df):
        fil = ( ((df['event']=='Time to unload each tote') & (df['task_status']=='unloading_at_storable'))  | ((df['event']=='Time to load each tote') & (df['task_status']=='loading_from_storable')) )
        df1 = df[fil]
        if df1.empty:
            return pd.DataFrame()
        df1['time_diff'] = df1.apply(lambda x: x['current_location'].split(",")[0].strip("{"),axis=1)
        df1['tote_order_count'] = df1.apply(lambda x: x['current_location'].split(",")[1].strip("{"),axis=1)
        group_df = df1.groupby(['installation_id','ranger_id', 'task_key'],as_index=False).agg(
            time_diff = ('time_diff','nunique'),
            tote_order_count = ('tote_order_count','nunique'))
        group_df['group_event'] = "Distinct Aisle"
        return group_df

    def get_transformed_df(self,df):
        groupby_list = ['installation_id','ranger_id', 'task_key','ndeep']
        final = self.cal_consolidated_info(df,'Time to load each tote','Time to load all totes',groupby_list)
        temp = self.cal_consolidated_info(df,'Time to unload each tote','Time To unload all totes',groupby_list)
        final = pd.concat([final,temp])
        groupby_list = ['installation_id','ranger_id', 'task_key']
        temp = self.cal_consolidated_info(df,'Wait time','total wait time',groupby_list)
        final = pd.concat([final,temp])
        temp = self.cal_consolidated_info(df,'roaming','total roaming time',groupby_list)
        final = pd.concat([final,temp])
        temp = self.cal_consolidated_info(df,'Waiting at conveyor before unloading','total wait at conveyor before unloading totes',groupby_list)
        final = pd.concat([final,temp])
        temp = self.cal_consolidated_info(df,'From storable to Conveyor','total time to reach conveyor',groupby_list)
        final = pd.concat([final,temp])
        temp = self.cal_consolidated_info(df,'Time to load tote from conveyor','total time to load all totes conveyor',groupby_list)
        final = pd.concat([final,temp])
        temp = self.cal_consolidated_info(df,'convenyor to storable','total s2s time to unload all totes storable',groupby_list)
        final = pd.concat([final,temp])
        temp = self.get_distinct_aisle(df)
        final = pd.concat([final,temp])
        temp2, temp3 = self.cal_time_to_reach_location_where_tote_loaded(df)
        final = pd.concat([final,temp2, temp3])
        final['ndeep'] = final['ndeep'].replace(np.nan, '0')
        final.rename(columns={'ndeep':'deep', 'time_diff':'total_time', 'tote_order_count':'count'},inplace=True)
        return final

    def func_time_to_reach_storable(self, df):
        tote_order_count = 0
        tote_cycle_group = 0
        for idx in df.index-1:
            if idx>=0:
                if tote_cycle_group != df['tote_cycle_group'][idx] or df['event'][idx] == 'task_assigned':
                    tote_order_count = 0

                # if df['new_event'][idx] == 'Time to reach storable' and df['event'][idx+1] == 'entity_loaded':
                if (df['new_event'][idx] == 'Time to reach storable') and ( df['event'][idx+1] == 'entity_loaded' or (idx+2 < len(df) and df['event'][idx+2] == 'entity_loaded')):
                    tote_order_count = tote_order_count + 1
                    tote_cycle_group = df['tote_cycle_group'][idx]
                    df['tote_order_count'][idx] = tote_order_count
        return df


    def func_time_to_load_each_tote(self,df):
        tote_order_count = 0
        tote_cycle_group = 0
        for idx in df.index:
            if tote_cycle_group != df['tote_cycle_group'][idx] or df['event'][idx] == 'task_assigned':
                tote_order_count = 0

            if df['new_event'][idx] == 'Time to load each tote' or df['new_event'][idx] == 'Time to load tote from conveyor':
                tote_order_count = tote_order_count + 1
                tote_cycle_group = df['tote_cycle_group'][idx]
                df['tote_order_count'][idx] = tote_order_count
        return df
    
    def func_storable_location_count(self,df):
        storable_location_count = 0
        tote_cycle_group = 0
        for idx in df.index:
            if tote_cycle_group != df['tote_cycle_group'][idx] or df['event'][idx] == 'task_assigned':
                storable_location_count = 0

            if df['new_event'][idx] == 'convenyor to storable' or df['new_event'][idx] == 'Time to reach storable':
                storable_location_count = storable_location_count + 1
                tote_cycle_group = df['tote_cycle_group'][idx]
                df['storable_location_count'][idx] = storable_location_count
        return df

    def func_time_to_unload_each_tote(self, df):
        tote_order_count = 0
        tote_cycle_group = 0
        for idx in df.index:
            if tote_cycle_group != df['tote_cycle_group'][idx] or df['event'][idx] == 'task_assigned':
                tote_order_count = 0

            if df['new_event'][idx] == 'Time to unload each tote':
                tote_order_count = tote_order_count + 1
                tote_cycle_group = df['tote_cycle_group'][idx]
                df['tote_order_count'][idx] = tote_order_count
        return df

    def func_conveyor_order_count(self, df):
        tote_conveyor_count = 0
        tote_cycle_group = 0
        for idx in df.index - 1:
            if idx>0:
                if tote_cycle_group != df['tote_cycle_group'][idx]:
                    tote_conveyor_count = 0

                if df['new_event'][idx] == 'From storable to Conveyor' and (df['event'][idx+1] == 'entity_unloaded' or df['event'][idx+1] == 'check_safe_to_drop'):
                    tote_conveyor_count = tote_conveyor_count + 1
                    tote_cycle_group = df['tote_cycle_group'][idx]
                    df['conveyor_order_count'][idx] = tote_conveyor_count
        return df

    def     apply_tote_cycle_group(self, df):
        group = 0
        for x in df.index:
            if x >= 1:
                if (df['ranger_id'][x] != df['ranger_id'][x - 1])  or df['task_key'][x] != df['task_key'][x - 1]:
                    group = group + 1
                df['tote_cycle_group'][x] = group
        return df

    def apply_add_cycle_time(self, df):
        for idx in df.index - 1:
            if idx >= 0:
                if df['new_event'][idx] == 'Time to load each tote':
                    if df['tote_order_count'][idx] == 1:
                        df['first_tote_load_time'][idx] = df['time'][idx]
                        df['last_tote_load_time'][idx] = df['time'][idx]
                    else:
                        df['last_tote_load_time'][idx] = df['time'][idx]
                elif df['event'][idx] == 'task_assigned':
                    df['task_assignment_time'][idx] = df['time'][idx]
                elif df['event'][idx] == 'task_completed':
                    df['task_task_completed'][idx] = df['time'][idx]
        return df


    def final_call(self,tenant_info,**kwargs):
        self.CommonFunction = CommonFunction()
        self.gcs = GCS()
        self.tenant_info = tenant_info['tenant_info']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.site = self.tenant_info['Name']
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("ttp_bot_journey", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        # self.end_date = datetime.now(timezone.utc)
        # self.final_call1(self.end_date, **kwargs)

        q = f"select *  from ranger_task_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("ttp_bot_journey", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("ttp_bot_journey", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
        
    def final_call1(self,end_date,**kwargs):
        # self.start_date = '2023-12-26T17:00:37.532902628Z'
        self.start_date = self.client.get_start_date("ttp_bot_journey", self.tenant_info)
        self.utilfunction = CommonFunction()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}")


        path = f'butler_nav_incidents/{self.tenant_info["Name"]}_bot_cycle.csv'
        path = os.path.join(CONFFILES_DIR, path)
        source_blob_name = f'{self.tenant_info["Name"]}_bot_cycle.csv'
        self.gcs.download_from_bucket(GOOGLE_BUCKET_NAME, source_blob_name, path)
        isExist = os.path.isfile(path)
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        collection_name = source_blob_name.replace(".csv", '')
        self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
        data = self.cls_mdb.get_two_days_data(self.start_date)
        selected_columns=['time', 'current_location', 'current_location_type', 'entity_id',
                           'entity_type', 'event', 'event_reason', 'fulfilment_area', 'host',
                           'installation_id', 'ndeep', 'next_destination', 'next_destination_type',
                           'ranger_id', 'load_on_ranger', 'ranger_version', 'task_key', 'task_status', 'task_type','journey_destination','journey_type']
        event_list = [['going_to_entity_location','reached_entity_location','loading_from_storable','Time to reach storable'],
                      ['task_assigned', 'entity_loaded', 'loading_from_storable', 'Time to reach storable'],
                      ['post_load_action_started', 'entity_loaded', 'loading_from_storable', 'Time to load each tote'],
                      ['entity_loading_started', 'entity_loaded', 'loading_from_storable', 'Time to load each tote'],
                      ['reached_entity_location', 'entity_loading_started', 'loading_from_storable', 'Wait before load'],
                      
                        ['reached_entity_location','entity_loaded','loading_from_storable','Time to load each tote'],
                        ['going_to_queue_point','reached_queue_point','loading_from_storable','Time to reach queue point'],
                        ['reached_queue_point','going_to_destination','unloading_at_conveyor','Time at queue point'],
                        ['reached_waiting_point','going_to_destination','unloading_at_conveyor','Time at wait point'],
                        ['reached_waiting_point','going_to_queue_point','loading_from_storable','Time at wait point'],
                        ['entity_loaded','going_to_queue_point','loading_from_storable','Time to fetch queue point'],
                        ['entity_loaded','going_to_waiting_point','loading_from_storable','Time to fetch wait point'],
                        ['entity_loaded','entity_loaded','loading_from_storable','Time to load each tote'],
                        ['waiting', 'entity_loaded', 'loading_from_storable', 'Time to load each tote'],
                        ['waiting','waiting_complete','loading_from_storable','Wait time'],
                        ['going_to_destination','reached_destination','unloading_at_conveyor','From storable to Conveyor'],
                        ['check_safe_to_drop','safe_to_drop','unloading_at_conveyor','Waiting at conveyor before unloading'],
                        ['check_safe_to_drop','unsafe_to_drop','unloading_at_conveyor','Waiting at conveyor before unloading'],
                        ['unsafe_to_drop','check_safe_to_drop','unloading_at_conveyor','Waiting at conveyor before unloading'],
                        ['reached_destination','check_safe_to_unload','unloading_at_conveyor','Waiting at conveyor before unloading'],
                        ['check_safe_to_unload','safe_to_unload','unloading_at_conveyor','Waiting at conveyor before unloading'],

                        ['safe_to_drop','entity_unloaded','unloading_at_conveyor','Time to unload each tote'],

                        ['task_completed','task_assigned','loading_from_storable','Bot Idle time'],
                        ['task_completed','task_assigned','loading_from_conveyor','Bot Idle time'],
                        ['going_to_entity_location','reached_entity_location','loading_from_conveyor','Time to reach conveyor'],
                        ['reached_entity_location','entity_loaded','loading_from_conveyor','Time to load tote from conveyor'],
                        ['entity_loaded','entity_loaded','loading_from_conveyor','Time to load tote from conveyor'],
                        ['waiting', 'entity_loaded', 'loading_from_conveyor','Time to load tote from conveyor'],
                        ['task_assigned', 'entity_loaded', 'loading_from_conveyor', 'Time to load tote from conveyor'],
                        ['entity_load_failure', 'entity_loaded', 'loading_from_conveyor','Time to load tote from conveyor'],
                        ['post_load_action_started', 'entity_loaded', 'loading_from_conveyor','Time to load tote from conveyor'],

                      ['waiting','waiting_complete','loading_from_conveyor','Wait time'],
                      ['going_to_destination','reached_destination','unloading_at_storable','convenyor to storable'],
                        ['reached_destination','entity_unloaded','unloading_at_storable','Time to unload each tote'],
                        ['entity_unloading_started','entity_unloaded','unloading_at_storable','Time to unload each tote'],
                        ['entity_unloaded', 'entity_unloaded', 'unloading_at_storable', 'Time to unload each tote'],
                        ['retry_safe_to_drop','safe_to_drop','unloading_at_conveyor','Waiting at conveyor before unloading'],
                        ['retry_safe_to_drop','check_safe_to_drop','unloading_at_conveyor','Waiting at conveyor before unloading'],
                        ['going_to_waiting_point','reached_waiting_point','loading_from_storable','Time to reach waiting point'],
                        ['reached_waiting_point','going_to_destination','unloading_at_conveyor','Time on waiting point'],
                        ['safe_to_unload','entity_unloaded_from_bot_to_port','unloading_at_conveyor','Time to unload totes to port'],
                        ['task_assigned','loading_totes_started','loading_from_conveyor','Wait before loading from port'],
                        ['loading_totes_started','entity_loaded_from_port_to_bot','loading_from_conveyor','Time to load totes from port']]
        df = pd.DataFrame(event_list, columns=['prev_event', 'event', 'task_status', 'new_event'])
        if data:
            cof = pd.DataFrame(data)
        elif isExist:
            cof = pd.read_csv(path)
        else:
            cof = pd.DataFrame(columns=selected_columns)

        if 'journey_destination' not in cof.columns:
            cof['journey_destination'] = ''
            cof['journey_type'] = ''
        cof = cof.drop_duplicates(subset=list(set(selected_columns)-set('time')), keep='first')
        if not cof.empty:
            cof = cof[cof['event'] != 'pre_load_unload_action_started']
            cof = cof[cof['event'] != 'pre_load_unload_action_ack_received']

        q = f"select * from ranger_task_events where time >'{self.start_date}' and time<='{self.end_date}' and (event='task_assigned' or event='task_completed')   order by time"
        ranger_task_events = pd.DataFrame(self.read_client.query(q).get_points())
        if ranger_task_events.empty:
            return None
        q = f"select * from ranger_task_events where time >='{self.start_date}' and time<='{self.end_date}'  and (event !='pre_load_unload_action_started' and event !='pre_load_unload_action_ack_received')  order by time"
        ranger_task_events = pd.DataFrame(self.read_client.query(q).get_points())
        if ranger_task_events.empty:
            filter = {"site": self.tenant_info['Name'], "table": "ttp_bot_journey"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return None
        ranger_task_events= pd.concat([ranger_task_events,cof])
        ranger_task_events=ranger_task_events[selected_columns]
        ranger_task_events = ranger_task_events[ranger_task_events['event']!='post_load_action_complete'].reset_index(drop=True)
        ranger_task_events = self.utilfunction.reset_index(ranger_task_events)
        ranger_task_events['time'] = pd.to_datetime(ranger_task_events['time'],utc=True)
        events = ['entity_id','entity_type','event_reason','ndeep','next_destination','next_destination_type']
        for event in events:
            ranger_task_events[event] = ranger_task_events[event].astype(str,errors='ignore')
            ranger_task_events[event] = ranger_task_events[event].apply(lambda x: 'null' if x == 'nan' else x)
        ranger_task_events['ndeep'] = ranger_task_events['ndeep'].astype(str, errors='ignore')
        ranger_task_events['ndeep'] = ranger_task_events['ndeep'].apply(lambda x: x[0] if (type(x) is str and len(x)>0) else x)
        ranger_task_events['ranger_id'] = ranger_task_events['ranger_id'].astype(int, errors='ignore')
        ranger_task_events = self.clean_journey_destination_type_cols(ranger_task_events)

        ranger_task_events = ranger_task_events.sort_values(by=['installation_id','ranger_id', 'time'], ascending=[True,True, True])
        ranger_task_events = self.utilfunction.reset_index(ranger_task_events)
        ranger_task_events = ranger_task_events.drop_duplicates(subset=selected_columns, keep='first')
        ranger_task_events = self.utilfunction.reset_index(ranger_task_events)
        ranger_task_events['tote_cycle_group'] = 0
        ranger_task_events = self.apply_tote_cycle_group(ranger_task_events)
        ranger_task_events['check_safe_to_drop'] = np.nan
        ranger_task_events['entity_loaded'] = np.nan
        ranger_task_events['entity_loaded_from_conveyor'] = np.nan
        ranger_task_events['entity_unloaded_from_storage'] = np.nan
        ranger_task_events['entity_unloaded'] = np.nan
        ranger_task_events['going_to_entity_location'] = np.nan
        ranger_task_events['going_to_destination'] = np.nan
        ranger_task_events['reached_entity_location'] = np.nan
        ranger_task_events['reached_destination'] = np.nan
        ranger_task_events['safe_to_drop'] = np.nan
        ranger_task_events['task_assigned'] = np.nan
        ranger_task_events['task_completed'] = np.nan
        ranger_task_events['waiting'] = np.nan
        ranger_task_events['waiting_complete'] = np.nan
        ranger_task_events['bot_reached_port_loading'] = np.nan
        ranger_task_events['bot_leaving_port_loading'] = np.nan
        ranger_task_events['bot_reached_port_unloading'] = np.nan
        ranger_task_events['bot_leaving_port_unloading'] = np.nan
        ranger_task_events['roaming_start'] = np.nan
        ranger_task_events['entity_loaded_from_port_to_bot'] = np.nan
        ranger_task_events['reached_queue_point'] = np.nan
        ranger_task_events['going_to_queue_point'] = np.nan
        ranger_task_events['is_port_journey'] = -1
        ranger_task_events['check_safe_to_drop'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'check_safe_to_drop' else np.nan, axis=1)
        ranger_task_events['entity_loaded'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'entity_loaded' and x['task_status'] == 'loading_from_storable' else np.nan, axis=1)
        ranger_task_events['entity_loaded_from_conveyor'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'entity_loaded' and x['task_status'] == 'loading_from_conveyor' else np.nan, axis=1)
        ranger_task_events['entity_unloaded_at_conveyor'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'entity_unloaded' and x['task_status'] == 'unloading_at_conveyor' else np.nan, axis=1)
        ranger_task_events['entity_unloaded_from_storage'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'entity_unloaded' and x['task_status'] == 'unloading_at_storable' else np.nan, axis=1)
        ranger_task_events['going_to_entity_location'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'going_to_entity_location' else np.nan, axis=1)
        ranger_task_events['going_to_destination'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'going_to_destination' else np.nan, axis=1)
        ranger_task_events['reached_entity_location'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'reached_entity_location' else np.nan, axis=1)
        ranger_task_events['reached_destination'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'reached_destination' else np.nan, axis=1)
        ranger_task_events['safe_to_drop'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'safe_to_drop' else np.nan, axis=1)
        ranger_task_events['task_assigned'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'task_assigned' else np.nan, axis=1)
        ranger_task_events['task_completed'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'task_completed' else np.nan, axis=1)
        ranger_task_events['waiting'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'waiting' else np.nan, axis=1)
        ranger_task_events['waiting_complete'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'waiting_complete' else np.nan, axis=1)
        ranger_task_events['reached_waiting_point'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'reached_waiting_point' else np.nan, axis=1)
        ranger_task_events['entity_loaded_from_port_to_bot'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'entity_loaded_from_port_to_bot' else np.nan, axis=1)
        ranger_task_events['bot_reached_port_loading'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'reached_entity_location' and x['current_location_type']=='port_exit_io_point' else np.nan, axis=1)
        ranger_task_events['bot_leaving_port_loading'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'going_to_destination' and x['current_location_type']=='port_exit_io_point' else np.nan, axis=1)
        ranger_task_events['bot_reached_port_unloading'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'reached_destination'  else np.nan, axis=1)
        ranger_task_events['bot_leaving_port_unloading'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'task_completed' else np.nan, axis=1)
        ranger_task_events['roaming_start'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'going_to_entity_location' and (x['next_destination']=='null' or pd.isna(x['next_destination'])) else np.nan, axis=1)
        ranger_task_events['safe_to_unload'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'safe_to_unload' and x['current_location_type']=='port_entry_io_point'  else np.nan, axis=1)
        ranger_task_events['reached_destination_early_dispatch'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'reached_destination' and x['task_type']=='early_dispatch_task'  else np.nan, axis=1)
        ranger_task_events['by_wait_point'] = ranger_task_events.apply(
            lambda x: 1 if x['event'] == 'reached_waiting_point'   else 0, axis=1)
        ranger_task_events['reached_queue_point'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'reached_queue_point'  else np.nan, axis=1)
        ranger_task_events['going_to_queue_point'] = ranger_task_events.apply(
            lambda x: x['time'] if x['event'] == 'going_to_queue_point'  else np.nan, axis=1)
        ranger_task_events['is_port_journey'] = ranger_task_events.apply(
            lambda x: 1 if x['current_location_type'] == 'port_entry_io_point' or x['current_location_type'] == 'port_exit_io_point' else x['is_port_journey'], axis=1)
        ranger_task_events['is_port_journey'] = ranger_task_events.apply(
            lambda x: 0 if x['current_location_type'] == 'conveyor_entry_io_point' or x['current_location_type'] == 'conveyor_exit_io_point' else x['is_port_journey'], axis=1)
        
        
        group_data = ranger_task_events.groupby(['installation_id','ranger_id', 'tote_cycle_group'], as_index=False).agg(
            new_time= ('task_completed', 'max'),
            cycle_start_time=('time', 'min'),
            cycle_end_time=('time', 'max'),
            first_tote_load_time=('entity_loaded', 'min'),
            last_tote_load_time=('entity_loaded', 'max'),
            task_assignment_time=('task_assigned', 'max'),
            task_completed_time=('task_completed', 'max'),
            going_to_entity_location_time=('going_to_entity_location', 'min'),
            reached_entity_location_time=('reached_entity_location', 'max'),
            waiting_time=('waiting', 'min'),
            waiting_complete_time=('waiting_complete', 'max'),
            safe_to_drop_time=('safe_to_drop', 'max'),
            first_tote_unload_time=('entity_unloaded_at_conveyor', 'min'),
            last_tote_unload_time=('entity_unloaded_at_conveyor', 'max'),
            first_tote_load_from_conveyor_time=('entity_loaded_from_conveyor', 'min'),
            last_tote_load_from_conveyor_time=('entity_loaded_from_conveyor', 'max'),
            first_entity_unloaded_from_storage_time=('entity_unloaded_from_storage', 'min'),
            last_entity_unloaded_from_storage_time=('entity_unloaded_from_storage', 'max'),
            going_to_destination_time=('going_to_destination','min'),
            reaching_port_loading_time = ('bot_reached_port_loading','min'),
            leaving_port_loading_time = ('bot_leaving_port_loading','max'),
            bot_reached_port_unloading_time = ('bot_reached_port_unloading','min'),
            bot_leaving_port_unloading_time = ('bot_leaving_port_unloading','max'),
            journey = ('journey_type','max'),
            roaming_start_time = ('roaming_start','min'),
            reached_waiting_point_time = ('reached_waiting_point','max'),
            reached_destination_time = ('reached_destination','max'),
            safe_to_unload_time = ('safe_to_unload','max'),
            entity_loaded_from_port_to_bot_time = ('entity_loaded_from_port_to_bot','min'),
            reached_destination_early_dispatch_time = ('reached_destination_early_dispatch','max'),
            wait_point = ('by_wait_point','max'),
            journey_destination_type = ('journey_destination','max'),
            reached_queue_point_time = ('reached_queue_point','max'),
            going_to_queue_point_time = ('going_to_queue_point','min'),
            journey_old = ('is_port_journey','max')
        )
        ranger_task_events = ranger_task_events.sort_values(by=['installation_id','ranger_id', 'tote_cycle_group', 'time'],
                                                            ascending=[True, True, True, True])
        ranger_task_events = self.utilfunction.reset_index(ranger_task_events)
        ranger_task_events['prev_time'] = ranger_task_events['time'].shift(1)
        ranger_task_events['prev_event'] = ranger_task_events['event'].shift(1)
        ranger_task_events['ranger_id'] = ranger_task_events['ranger_id'].astype(int, errors='ignore')
        ranger_task_events['prev_ranger_id'] = ranger_task_events['ranger_id'].shift(1)
        ranger_task_events['prev_ranger_id'] = ranger_task_events['prev_ranger_id'].fillna(0).astype(int)
        ranger_task_events = ranger_task_events[((ranger_task_events['event'] != ranger_task_events['prev_event']) | (
                    ranger_task_events['ranger_id'] != ranger_task_events['prev_ranger_id']) | (ranger_task_events['event']=='entity_loaded') | (ranger_task_events['event']=='entity_unloaded'))]
        ranger_task_events=self.utilfunction.reset_index(ranger_task_events)
        ranger_task_events['prev_time'] = ranger_task_events['time'].shift(1)
        ranger_task_events['prev_event'] = ranger_task_events['event'].shift(1)
        ranger_task_events['prev_ranger_id'] = ranger_task_events['ranger_id'].shift(1)
        ranger_task_events['prev_ranger_id'] = ranger_task_events['prev_ranger_id'].fillna(0).astype(int)

        ranger_task_events['time_diff'] = np.nan
        ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'time', 'prev_time', 'time_diff')
        ranger_task_events['time_diff'] = ranger_task_events.apply(
            lambda x: x['time_diff'] if x['ranger_id'] == x['prev_ranger_id'] and not pd.isna(x['time_diff']) else 0,
            axis=1)
        ranger_task_events = pd.merge(ranger_task_events, df, how='left',
                                      on=['event', 'prev_event', 'task_status'])
        ranger_task_events['new_event'] = ranger_task_events.apply(lambda x: 'roaming' if (x['next_destination']=='null' or pd.isna(x['next_destination'])) and x['new_event']=='Time to reach storable' and x['event']=='reached_entity_location' else x['new_event'], axis=1)
        ranger_task_events['tote_order_count'] = 0
        ranger_task_events['conveyor_order_count'] = 0
        ranger_task_events['storable_location_count'] = 0
        ranger_task_events = self.func_storable_location_count(ranger_task_events)
        ranger_task_events = self.func_time_to_load_each_tote(ranger_task_events)
        ranger_task_events = self.func_time_to_reach_storable(ranger_task_events)
        ranger_task_events = self.func_time_to_unload_each_tote(ranger_task_events)
        ranger_task_events = self.func_conveyor_order_count(ranger_task_events)
        ranger_task_events['new_event']= ranger_task_events.apply(lambda x:'Time taken from one conveyor to other' if not pd.isna(x['conveyor_order_count']) and x['conveyor_order_count']>1 else x['new_event'], axis=1 )
        group_data['ranger_id'] = group_data['ranger_id'].fillna(0).astype(int)
        ranger_task_events = pd.merge(ranger_task_events, group_data, how='left', on=['installation_id','ranger_id', 'tote_cycle_group'])
        ranger_task_events = ranger_task_events[~(pd.isna(ranger_task_events['task_assignment_time']))]
        if ranger_task_events.empty:
            return
        ranger_task_events['task_cycle_time'] = np.nan
        ranger_task_events['first_entity_loaded_time_diff'] = np.nan
        ranger_task_events['last_entity_loaded_time_diff'] = np.nan
        ranger_task_events['total_unload_time_diff'] = np.nan
        ranger_task_events['wait_time_diff'] = np.nan
        ranger_task_events['total_load_time_from_conveyor_time_diff'] = np.nan
        ranger_task_events['entity_unloaded_from_storage_time_diff'] = np.nan
        ranger_task_events['Total_Time_to_reach_conveyor'] = np.nan
        ranger_task_events['time_diff_last_load_to_assign'] = np.nan
        ranger_task_events['all_tote_loaded_to_task_completed'] = np.nan
        ranger_task_events['time_bot_spend_on_port_loading'] = np.nan
        ranger_task_events['time_bot_spend_on_port_unloading'] = np.nan
        ranger_task_events['roaming_to_task_completed'] = np.nan
        ranger_task_events['wait_at_port_before_unloading'] = np.nan
        ranger_task_events['task_assign_to_tote_load_port'] = np.nan
        ranger_task_events['journey_variant'] = np.nan
        ranger_task_events['time_to_reach_conveyor'] = np.nan
        ranger_task_events['journey_variant'] = ranger_task_events.apply(lambda x: self.find_journey_variant(x['reached_queue_point_time'],x['reached_waiting_point_time']), axis = 1)

        save_not_complete_task = ranger_task_events[pd.isna(ranger_task_events['task_completed_time']) & pd.isna(ranger_task_events['reached_destination_early_dispatch_time'])]
        early_dispatch_tasks = ranger_task_events[(~pd.isna(ranger_task_events['reached_destination_early_dispatch_time']))]
        ranger_task_events = ranger_task_events[(~pd.isna(ranger_task_events['task_completed_time']))]

        if not early_dispatch_tasks.empty:
            final_df = self.time_to_reach_destination_ealy_dispactch(early_dispatch_tasks)
            final_df = self.utilfunction.handle_multiple_dtype(self.client, final_df, "early_dispatch_tasks_time_diff","load_on_ranger")
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                     port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(final_df, "early_dispatch_tasks_time_diff", db_name=self.tenant_info["alteryx_out_db_name"],
                                              tag_columns=['event'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                

        if not ranger_task_events.empty:
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'task_completed_time',
                                                                'task_assignment_time', 'task_cycle_time')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'first_tote_load_time',
                                                                'task_assignment_time', 'first_entity_loaded_time_diff')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'last_tote_load_time',
                                                                'task_assignment_time', 'last_entity_loaded_time_diff')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'last_tote_load_from_conveyor_time',
                                                                'task_assignment_time', 'time_diff_last_load_to_assign')

            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'waiting_complete_time',
                                                                'waiting_time', 'wait_time_diff')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'last_tote_unload_time',
                                                                'first_tote_unload_time', 'total_unload_time_diff')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'last_tote_load_from_conveyor_time',
                                                                'first_tote_load_from_conveyor_time', 'total_load_time_from_conveyor_time_diff')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'last_entity_unloaded_from_storage_time',
                                                                'first_entity_unloaded_from_storage_time', 'entity_unloaded_from_storage_time_diff')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'task_completed_time',
                                                                'going_to_destination_time', 'all_tote_loaded_to_task_completed')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'reached_entity_location_time',
                                                                'going_to_entity_location_time', 'Total_Time_to_reach_conveyor')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'leaving_port_loading_time',
                                                                'reaching_port_loading_time', 'time_bot_spend_on_port_loading')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'bot_leaving_port_unloading_time',
                                                                'bot_reached_port_unloading_time', 'time_bot_spend_on_port_unloading')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'task_completed_time',
                                                                'last_tote_load_time', 'roaming_to_task_completed')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'safe_to_unload_time',
                                                                'reached_destination_time', 'wait_at_port_before_unloading')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'reached_destination_time',
                                                                'last_tote_load_time', 'time_to_reach_conveyor')
            ranger_task_events = self.utilfunction.datediff_in_millisec(ranger_task_events, 'entity_loaded_from_port_to_bot_time',
                                                                'task_assignment_time', 'task_assign_to_tote_load_port')
            
            
            ranger_task_events['new_event'] = ranger_task_events.apply(
                lambda x: 'Misc_' + str(x['event']) + '_' + str(x['prev_event']) if pd.isna(x['new_event']) else x[
                    'new_event'], axis=1)
            ranger_task_events['journey_old'] = ranger_task_events.apply(lambda x:'port' if x['journey_old']==1 else ('conveyor' if  x['journey_old']==0 else 'No Port No Conveyor') ,axis=1)
            ranger_task_events['journey'] = ranger_task_events.apply(lambda x: x['journey_old'] if (pd.isna(x['journey']) or x['journey']=='' ) else x['journey'], axis = 1)

        # save_not_complete_task = ranger_task_events[pd.isna(ranger_task_events['task_completed_time'])]
        temp_df = save_not_complete_task.groupby(['ranger_id'], as_index=False).agg(
            task_assignment_time=('task_assignment_time', 'max')).reset_index()
        save_not_complete_task = pd.merge(save_not_complete_task, temp_df, on=['ranger_id','task_assignment_time'], how='inner')
        save_not_complete_task = save_not_complete_task[selected_columns]
        selected_list=['time', 'installation_id', 'host', 'new_event', 'event_reason', 'ranger_id', 'time_diff',
            'tote_order_count', 'load_on_ranger', 'conveyor_order_count', 'storable_location_count', 'task_status','new_time','current_location','task_assignment_time','task_key','ndeep','journey','journey_destination_type','journey_variant']
        # ranger_task_events = ranger_task_events[
        #     (ranger_task_events['ranger_id'] == ranger_task_events['prev_ranger_id'])]
        # ranger_task_events = ranger_task_events[(~pd.isna(ranger_task_events['task_completed_time']))]
        if ranger_task_events.empty:
            if self.cls_mdb.delete_data({}, multiple=True):
                if not save_not_complete_task.empty:
                    save_not_complete_task['time'] = save_not_complete_task['time'].apply(lambda x: self.CommonFunction.convert_to_nanosec(x))
                    json_list = save_not_complete_task.to_json(orient="records")
                    json_list = json.loads(json_list)
                    test = self.cls_mdb.insert_data(json_list)
                else:
                    save_not_complete_task.to_csv(path, index=False, header=True)
                    self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
                #print(test)
            # save_not_complete_task.to_csv(path, index=False, header=True)
            # self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
            filter = {"site": self.tenant_info['Name'], "table": "ttp_bot_journey"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return None
        
        ranger_task_events['new_event'] = ranger_task_events.apply(lambda x:'From waiting point to conveyor' if x['new_event']=='From storable to Conveyor' and not (pd.isna(x['reached_waiting_point_time'])) else x['new_event'] ,axis=1)
            
        final_df = ranger_task_events[selected_list]
        final_df['calc'] = 0
        ranger_task_events['calc'] = 1
        final_df2 = ranger_task_events[(ranger_task_events['event']=='task_assigned') & (~pd.isna(ranger_task_events['task_cycle_time']))]
        final_df2['new_event'] = 'Bot Cycle time'
        final_df2['time_diff'] = final_df2['task_cycle_time']
        combine_df = final_df2[selected_list]
        final_df2['new_event'] = 'Task Assignment to First Tote load time'
        final_df2['time_diff'] = final_df2['first_entity_loaded_time_diff']

        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        final_df2['new_event'] = 'Task Assignment to Last Tote load time'
        final_df2['time_diff'] = final_df2['last_entity_loaded_time_diff']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])
        final_df2['new_event'] = 'time to unload all totes'
        final_df2['time_diff'] = final_df2['entity_unloaded_from_storage_time_diff']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        final_df2['new_event'] = 'Total time to unload all totes'
        final_df2['time_diff'] = final_df2['total_unload_time_diff']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        final_df2['new_event'] = 'Total Time to reach conveyor'
        final_df2['time_diff'] = final_df2['Total_Time_to_reach_conveyor']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])
        
        final_df2['new_event'] = 'all tote load to task completed time'
        final_df2['time_diff'] = final_df2['all_tote_loaded_to_task_completed']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])


        final_df2['new_event'] ='Time to load all totes from conveyor'
        final_df2['time_diff'] = final_df2['total_load_time_from_conveyor_time_diff']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        final_df2['new_event'] = 'Task Assignment to Last Tote load time from conveyor'
        final_df2['time_diff'] = final_df2['time_diff_last_load_to_assign']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        final_df2['new_event'] = 'Time bot was on port loading'
        final_df2['time_diff'] = final_df2['time_bot_spend_on_port_loading']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        final_df2['new_event'] = 'Time bot was on port unloading'
        final_df2['time_diff'] = final_df2['time_bot_spend_on_port_unloading']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        final_df2['new_event'] = 'Roaming to task completed'
        final_df2['time_diff'] = final_df2['roaming_to_task_completed']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])        

        final_df2['new_event'] = 'Wait at port before unloading'
        final_df2['time_diff'] = final_df2['wait_at_port_before_unloading']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        final_df2['new_event'] = 'Task assign to tote load port'
        final_df2['time_diff'] = final_df2['task_assign_to_tote_load_port']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        final_df2['new_event'] = 'Last entity Loaded to reach destination'
        final_df2['time_diff'] = final_df2['time_to_reach_conveyor']
        combine_df2 = final_df2[selected_list]
        combine_df = pd.concat([combine_df, combine_df2])

        combine_df = self.utilfunction.reset_index(combine_df)
        for x in combine_df.index:
            combine_df['time'][x] = pd.to_datetime(combine_df['time'][x],utc=True) + timedelta(milliseconds=x)

        final_df = pd.concat([final_df, combine_df])
        final_df = self.utilfunction.reset_index(final_df)
        final_df.rename(columns={'new_event': 'event','time': 'event_time','new_time': 'time', }, inplace=True)
        final_df.rename(columns={'new_time': 'time', }, inplace=True)
        final_df.dropna(subset=['time'], inplace=True)
        final_df['task_completed_time']=final_df['time']
        final_df = final_df.sort_values(by=['ranger_id', 'event_time'], ascending=[True, False])
        final_df = self.utilfunction.reset_index(final_df)

        temp = self.get_transformed_df(final_df)
        temp.rename(columns={'ndeep':'deep'},inplace=True)
        temp = temp.reset_index()
        if 'index' in temp.columns:
            del temp['index']
        temp_final_df2 = final_df2[selected_list]
        temp = pd.merge(temp_final_df2,temp,on=['installation_id','ranger_id','task_key'],how = 'left')
        temp.drop(['tote_order_count','ndeep','time_diff','new_event'],axis=1, inplace=True)
        temp.rename(columns={'deep':'ndeep', 'total_time':'time_diff', 'count':'tote_order_count', 'group_event':'event','time':'event_time','new_time':'time'},inplace=True)
        temp['calc'] = 1
        final_df = pd.concat([final_df,temp])

        final_df['task_completed_time']=final_df['time']
        final_df = final_df.sort_values(by=['task_key', 'event_time'], ascending=[True, True])
        final_df = self.utilfunction.reset_index(final_df)
        if 'index' in final_df.columns:
            del final_df['index']
    
        k=0
        for x in final_df.index:
            if x>0:
                if final_df['task_completed_time'][x]==final_df['task_completed_time'][x-1]:
                    final_df['time'][x] = pd.to_datetime(final_df['time'][x],utc=True) - timedelta(milliseconds=k)
                    k= k+1
                else:
                    k=1
                    
        if not final_df2.empty:
            final_df['check_data'] = final_df['time_diff'].apply(lambda x: True if x != 0.0 and not pd.isna(x) else False)
            final_df = final_df[final_df['check_data']]
            final_df = self.utilfunction.reset_index(final_df)
            if not final_df.empty:
                del final_df['check_data']
                final_df['time_ns']=final_df['time'].apply(lambda x: x.timestamp())
                final_df['time_ns']=final_df['time_ns']* 1e9
                final_df = final_df.set_index('time')
                final_df.event_time = final_df.event_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
                final_df.task_assignment_time = final_df.task_assignment_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f%z'))
                final_df.task_completed_time = final_df.task_completed_time.apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f%z'))
                if 'index' in final_df.columns:
                    del final_df['index']

                if 'level_0' in final_df.columns:
                    del final_df['level_0']
                final_df['x_coordinate'] = final_df['current_location'].apply(lambda x: x.split(",")[0].strip("{") if (~pd.isna(x) and x!=None and '{' in x) else -1)                
                final_df['y_coordinate'] = final_df['current_location'].apply(lambda x: x.split(",")[1].strip("}") if (~pd.isna(x) and x!=None and '}' in x) else -1)
                final_df['x_coordinate'] = final_df.apply(lambda x: x['time_diff'] if x['event']=='Distinct Aisle' else x['x_coordinate'],axis=1)
                final_df['y_coordinate'] = final_df.apply(lambda x: x['tote_order_count'] if x['event']=='Distinct Aisle' else x['y_coordinate'],axis=1)
                final_df['ranger_id'] = final_df['ranger_id'].astype(str , errors='ignore')
                final_df['load_on_ranger'] = final_df['load_on_ranger'].astype(float , errors='ignore')
                final_df['task_key'] = final_df['task_key'].astype(str , errors='ignore')
                final_df['ndeep'] = final_df['ndeep'].astype(str, errors='ignore')
                final_df['time_diff'] = final_df['time_diff'].astype(float, errors='ignore')
                final_df['tote_order_count'] = final_df['tote_order_count'].astype(int, errors='ignore')
                final_df['conveyor_order_count'] = final_df['conveyor_order_count'].astype(int, errors='ignore')
                final_df['storable_location_count'] = final_df['storable_location_count'].astype(int, errors='ignore')
                final_df['x_coordinate'] = final_df['x_coordinate'].astype(int , errors='ignore')
                final_df['y_coordinate'] = final_df['y_coordinate'].astype(int , errors='ignore')
                final_df['journey'] = final_df['journey'].astype(str , errors='ignore')
                final_df['calc'] = final_df['calc'].astype(int, errors='ignore')
                print("inserted")

                # print(final_df.columns)
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                     port=self.tenant_info["write_influx_port"])
                self.write_client.writepoints(final_df, "ttp_bot_journey", db_name=self.tenant_info["alteryx_out_db_name"],
                                              tag_columns=['event'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                if not save_not_complete_task.empty and not final_df.empty:
                    if self.cls_mdb.delete_data({}, multiple=True):
                        save_not_complete_task['time'] = save_not_complete_task['time'].apply(lambda x: self.CommonFunction.convert_to_nanosec(x))
                        json_list = save_not_complete_task.to_json(orient="records")
                        json_list = json.loads(json_list)
                        test = self.cls_mdb.insert_data(json_list)
                else:
                        save_not_complete_task.to_csv(path, index=False, header=True)
                        self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
                        #print(test)
                    #final_df = final_df.sort_values(by=['time'], ascending=[False])
                    #final_df = self.utilfunction.reset_index(final_df)
                    #max_time = final_df['time'][0]
                    # save_not_complete_task= save_not_complete_task[save_not_complete_task['time']<max_time]
                    # save_not_complete_task.to_csv(path, index=False, header=True)
                    # self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'ttp_bot_journey',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'TTP Bot Journey ',
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
            if tenant['Active'] == "Y" and tenant['ttp_bot_journey'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='ttp_bot_journey_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(ttp_bot_journey().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
            if tenant['Active'] == "Y" and tenant['ttp_bot_to_bot_time'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Bot_to_bot_time_final_task = PythonOperator(
                    task_id='bot_to_bot_time_calcuation_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(Bot_To_Bot_Time().final_call,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),)
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='ttp_bot_journey_final',
                provide_context=True,
            python_callable=functools.partial(ttp_bot_journey().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )