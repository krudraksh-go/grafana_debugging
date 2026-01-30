import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, GCS
import os, json
from utils.CommonFunction import MongoDBManager
from config import (
    MongoDbServer,
    rp_seven_days
)


class RelayBotJourney:

    def save_last_entry(self):
        filter = {"site": self.tenant_info['Name'], "table": "relay_bot_journey"} 
        new_last_run = {"last_run": self.end_date}
        self.CommonFunction.update_dag_last_run(filter,new_last_run)     

    def final_call(self, tenant_info, **kwargs):
        self.CommonFunction = CommonFunction()
        self.gcs = GCS()
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["write_influx_ip"],
                                      port=self.tenant_info["write_influx_port"], db=self.tenant_info["out_db_name"])
        self.site = self.tenant_info['Name']
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("relay_bot_journey", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from ranger_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("relay_bot_journey", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("relay_bot_journey", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def fetch_previous_incomplete_cycles(self):
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        collection_name = f'{self.tenant_info["Name"]}_ranger_cycle'
        self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
        data = self.cls_mdb.get_previous_hours_data(self.start_date, hours=10)
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if '_id' in df.columns:
            del df['_id']
        return df

    def fetch_raw_data(self):
        q = f"select *  from ranger_events where time>'{self.start_date}' and time<='{self.end_date}' "
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if not df.empty:
            previous_incomplete_cycles = self.fetch_previous_incomplete_cycles()
            if not previous_incomplete_cycles.empty:
                df = pd.concat([df, previous_incomplete_cycles], ignore_index=True)
            df = df.sort_values(by=['installation_id', 'ranger_id', 'task_key', 'time'],
                                ascending=[True, True, True, True]).reset_index(drop=True)
            df = df.fillna('')
            df = df.replace('null', '')
            unique_events = df['event'].unique()
            known_events = ["task_assigned", "going_to_destination", "reached_destination","action_started", "action_finished", "task_completed", "subtask_started", "subtask_completed","loading_completed", "unloading_completed"]
            unknown_events = list(set(unique_events) - set(known_events))
            if unknown_events:
                raise ValueError(f"Unknown events found: {unknown_events}")            
            df = self.remove_incomplete_cycles(df)
            selected_cols = ['time','action_type','current_coordinate','current_coordinate_type','current_location','current_location_depth','current_location_type','event','host','installation_id','max_ranger_height','ranger_id','ranger_type','task_key','task_status','current_fork_height','current_load_count','next_destination_aisle_id','final_destination_coordinate','final_destination_coordinate_type','next_destination_coordinate','subtask_key']
            df = df[selected_cols]
            df = df[~((df['event'].isin(['task_assigned', 'task_completed']) & (df['ranger_type']=='vtm')))].reset_index(drop=True)
            df['event'] = df['event'].replace('subtask_started', 'task_assigned')
            df['event'] = df['event'].replace('subtask_completed', 'task_completed')                
        return df
    
    def add_ndeep(self, df):
        df['current_location_depth'] = df['current_location_depth'].replace('', 0)
        df["ndeep"] = df.groupby(['task_key','subtask_key'])['current_location_depth'].transform("max")
        df['current_location_depth'] = df['ndeep'].astype(int).astype(str)
        cols_to_del = ['ndeep']
        for col in cols_to_del:
            del df[col]
        return df


    def remove_unnecessary_events(self, df):
        condition1 = (df['event'].isin(['task_assigned', 'task_completed','action_finished','loading_completed','unloading_completed']))
        condition2 = (
            (df['event'] == 'reached_destination') &
            (
                (df['final_destination_coordinate_type'].isin(['relay_storable_io_point','ttp_storable_io_point','pps'])) |
                (df['current_coordinate'] == df['final_destination_coordinate']) |
                ( (df['final_destination_coordinate'].isin(['notfound','pps'])) &  (df['current_coordinate'] == df['next_destination_coordinate'])) 
            )
        )
        condition3 = ((df['event']=='going_to_destination') & ((df['current_coordinate_type']=='pps') & (df['final_destination_coordinate_type'].isin(['ttp_storable_io_point','relay_storable_io_point']))))

        flag = (condition1 | condition2 | condition3)
        df = df[flag].reset_index(drop=True)
        
        # event, task_status, current_coordinate_type, action_type, new_event
        new_event_map = [
            # HTM Journey
            ["reached_destination", "assigned", "relay_storable_io_point", "", "reached_io_point_s2p"],
            ["reached_destination", "assigned", "relay_storable", "", "reached_dock_point_s2p"],
            ["action_finished", "tote_picked", "relay_storable", "loading", "tote_loaded"],
            ["reached_destination", "tote_picked", "pps", "", "reached_pps"],
            ["going_to_destination", "storing", "pps", "", "going_pps_exit"],
            ["reached_destination", "storing", "pps_exit_queue", "", "reached_pps_exit"],
            ["reached_destination", "storing", "relay_storable_io_point", "", "reached_io_point_p2s"],
            ["reached_destination", "storing", "relay_storable", "", "reached_dock_point_p2s"],
            ["action_finished", "storing", "relay_storable", "unloading", "tote_unloaded"],

            # VTM S2R
            ["reached_destination", "loading_from_storable", "ttp_storable_io_point", "", "reached_storable"],
            ["loading_completed", "loading_from_storable", "ttp_storable_io_point", "loading", "tote_loaded"],
            ["reached_destination", "unloading_at_relay", "ttp_storable_io_point", "", "reached_relay_point"],
            ["unloading_completed", "unloading_at_relay", "ttp_storable_io_point", "unloading", "tote_unloaded"],
            # VTM R2S
            ["reached_destination", "loading_from_relay", "ttp_storable_io_point", "", "reached_relay_point"],
            ["loading_completed", "loading_from_relay", "ttp_storable_io_point", "loading", "tote_loaded"],
            ["reached_destination", "unloading_at_storable", "ttp_storable_io_point", "", "reached_storable"],
            ["reached_destination", "unloading_at_storable", "highway", "", "reached_storable"],
            ["unloading_completed", "unloading_at_storable", "ttp_storable_io_point", "unloading", "tote_unloaded"],
        ]
        new_event_df = pd.DataFrame(new_event_map,
                                    columns=['event', 'task_status', 'current_coordinate_type', 'action_type',
                                             'new_event'])
        df = pd.merge(df, new_event_df, on=['event', 'task_status', 'current_coordinate_type', 'action_type'],
                      how='left')
        df['new_event'] = df.apply(lambda x : x['event'] if x['event'] in ['task_assigned', 'task_completed'] else x['new_event'], axis=1)
        df = df[~pd.isna(df['new_event'])]
        df['original_event'] = df['event']
        df['event'] = df['new_event']
        if 'new_event' in df.columns:
            del df['new_event']
        return df.reset_index(drop=True)

    def remove_duplicate_entries(self, df):
        first_cocurence = ['task_assigned', 'tote_loaded', 'going_pps_exit']
        last_occurence = ['reached_pps', 'reached_pps_exit', 'reached_io_point_s2p', 'reached_dock_point_s2p','reached_io_point_p2s', 'reached_dock_point_p2s',  'task_completed',
                          'reached_relay_point', 'reached_storable', 'tote_unloaded']
        if not df.empty:
            df = df.sort_values(by=['installation_id','ranger_type', 'ranger_id', 'task_key', 'time'],
                                ascending=[True, True, True, True, True]).reset_index(drop=True)
            df['prev_event'] = df['event'].shift(1)
            df['next_event'] = df['event'].shift(-1)
            df['delete_mask'] = 0
            df['delete_mask'] = df.apply(
                lambda x: 1 if (x['event'] == x['prev_event'] and x['event'] in (first_cocurence)) or (
                            x['event'] == x['next_event'] and x['event'] in (last_occurence)) else 0, axis=1)
            df = df[df['delete_mask'] == 0].reset_index(drop=True)
            del df['delete_mask']
        return df

    def save_incomplete_cycles(self, df):
        cols_to_delete = ['start_time', 'end_time', 'cycle_start_time', 'cycle_end_time']
        for col in cols_to_delete:
            if col in df.columns:
                del df[col]
        if self.cls_mdb.delete_data({}, multiple=True):
            df['time'] = df['time'].apply(lambda x: self.CommonFunction.convert_to_nanosec(x))
            json_list = df.to_json(orient="records")
            json_list = json.loads(json_list)
            test = self.cls_mdb.insert_data(json_list)

    def remove_incomplete_cycles(self, df):
        df = df.sort_values(by=['installation_id','ranger_type', 'ranger_id', 'task_key', 'time'],
                                ascending=[True, True, True, True, True]).reset_index(drop=True)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df['start_time'] = np.nan
        df['end_time'] = np.nan
        df['start_time'] = df.apply(lambda x: x['time'] if x['event'] == 'task_assigned' else np.nan, axis=1)
        df['end_time'] = df.apply(lambda x: x['time'] if x['event'] == 'task_completed' else np.nan, axis=1)
        groupdf = df.groupby(['installation_id', 'ranger_type', 'ranger_id', 'task_key'], as_index=False).agg(
                cycle_start_time=('start_time', 'min'),
                cycle_end_time=('end_time', 'max'))
        df = pd.merge(df, groupdf, on=['installation_id', 'ranger_type', 'ranger_id', 'task_key'], how='left')
            
        incomplete_cycles = df[(pd.isna(df['cycle_start_time'])) | (pd.isna(df['cycle_end_time']))].reset_index(
                drop=True)
        complete_cycles = df[(~pd.isna(df['cycle_start_time'])) & (~pd.isna(df['cycle_end_time']))].reset_index(
                drop=True)
        # save incomplete cycles to mongodb
        self.save_incomplete_cycles(incomplete_cycles)
        return complete_cycles

    def get_time_diff_with_prev_event(self, df):
        df = df.sort_values(by=['installation_id','ranger_type', 'ranger_id', 'task_key', 'time'],
                            ascending=[True, True, True, True, True]).reset_index(drop=True)
        if 'next_destination_aisle_id' not in df.columns:
            df['next_destination_aisle_id'] = ''
        df = df.rename(columns={'next_destination_aisle_id':'aisle_id'})
        df['prev_time'] = df['time'].shift(1)
        df['prev_task_key'] = df['task_key'].shift(1)
        df['prev_installation_id'] = df['installation_id'].shift(1)
        df['prev_task_key'] = df['prev_task_key'].fillna('').astype(str)
        df['time_diff'] = np.nan
        df = self.CommonFunction.datediff_in_millisec(df, 'time', 'prev_time', 'time_diff')
        df['time_diff'] = df.apply(
            lambda x: x['time_diff'] if x['task_key'] == x['prev_task_key'] and not pd.isna(x['time_diff']) and x['installation_id'] == x['prev_installation_id'] else 0,
            axis=1)
        if 'index' in df.columns:
            del df['index']
        return df

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

    def write_raw_data_to_influx(self, df):
        selected_cols = ['time', 'event', 'host', 'installation_id', 'ranger_id', 'ranger_type', 'task_key','action_type',
                         'task_status', 'time_diff', 'current_coordinate', 'current_coordinate_type','aisle_id','subtask_key']
        relay_journey_raw = df[selected_cols].set_index('time')
        float_cols = ['time_diff']
        relay_journey_raw = self.typecast_to_float(relay_journey_raw, float_cols)
        str_cols = [col for col in selected_cols if col not in float_cols]
        relay_journey_raw = self.typecast_to_str(relay_journey_raw, str_cols)

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(relay_journey_raw, "relay_journey_raw",
                                      db_name=self.tenant_info["alteryx_out_db_name"],
                                      tag_columns=['host','installation_id','ranger_id','ranger_type','aisle_id'],
                                      dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy=rp_seven_days)

    def create_event_df(self):
        event_list = [
            # HTM
            ['task_assigned', 'reached_io_point_s2p', 'assigned', 'Time to Reach IO Point'],
            ['reached_io_point_s2p', 'reached_dock_point_s2p', 'assigned', 'Time to Reach IO Point'],
            ['reached_dock_point_s2p', 'tote_loaded', 'tote_picked', 'Time to Lift a tote'],
            ['tote_loaded', 'reached_pps', 'tote_picked', 'Time to Reach PPS'],
            ['reached_pps', 'going_pps_exit', 'storing', 'Time to Pick from Tote'],
            ['going_pps_exit', 'reached_pps_exit', 'storing', 'Time to Exit from PPS'],
            ['reached_pps_exit', 'reached_io_point_p2s', 'storing', 'Time from PPS to IO Point'],
            ['reached_io_point_p2s', 'reached_dock_point_p2s', 'storing', 'Time to Reach Dock Point Unloading'],
            ['reached_dock_point_p2s', 'tote_unloaded', 'storing', 'Time to drop tote'],
            ['tote_unloaded', 'task_completed', 'storing', 'Time to come back to IO Point'],
            ['tote_unloaded', 'reached_io_point_p2s', 'storing', 'Time to come back to IO Point'],
            ['reached_io_point_p2s', 'task_completed', 'storing', 'Time to come back to IO Point'],

            # VTM (S2R)
            ['task_assigned', 'reached_storable', 'loading_from_storable', 'Time to travel to IO Point'],
            ['reached_storable', 'tote_loaded', 'loading_from_storable', 'Time to pull tote towards itself'],
            ['tote_loaded', 'reached_relay_point', 'unloading_at_relay', 'Time to reach relay point'],
            ['reached_relay_point', 'tote_unloaded', 'unloading_at_relay', 'Time to place the tote at assigned relay point'],

            # VTM (R2S)
            ['task_assigned', 'reached_relay_point', 'loading_from_relay', 'Time to reach the tote to be sent to storable'],
            ['reached_relay_point', 'tote_loaded', 'loading_from_relay', 'Time to pull tote towards itself'],
            ['tote_loaded', 'reached_storable', 'unloading_at_storable', 'Time to travel to Tote storage area'],
            ['reached_storable', 'tote_unloaded', 'unloading_at_storable', 'Time to drop the tote in its designated slot'],
        ]
        df = pd.DataFrame(event_list, columns=['prev_event', 'event', 'task_status', 'new_event'])
        return df

    def create_metrics(self, df):
        event_df = self.create_event_df()
        df['prev_event'] = df['event'].shift(1)
        df['prev_time'] = df['time'].shift(1)
        df['time_diff'] = np.nan
        df = self.CommonFunction.datediff_in_millisec(df, 'time', 'prev_time', 'time_diff')
        df = pd.merge(df, event_df, on=['prev_event', 'event', 'task_status'], how='left')
        df['new_event'] = df['new_event'].apply(lambda x: 'Others' if pd.isna(x) else x)
        df = df.rename(columns={'event': 'intermediate_event', 'new_event': 'event'})
        return df

    def write_metrics_to_influx(self, df):
        selected_cols = ['time', 'original_event', 'event', 'current_coordinate', 'current_coordinate_type',
                         'current_fork_height', 'current_load_count', 'current_location', 'current_location_depth',
                         'current_location_type', 'intermediate_event', 'host', 'installation_id', 'max_ranger_height',
                         'ranger_id', 'ranger_type', 'task_key', 'task_status','time_diff']
        relay_journey = df[selected_cols].set_index('time')
        float_cols = ['time_diff','max_ranger_height','current_fork_height']
        relay_journey = self.typecast_to_float(relay_journey, float_cols)
        str_cols = [col for col in selected_cols if col not in float_cols]
        relay_journey = self.typecast_to_str(relay_journey, str_cols)
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(relay_journey, "relay_journey", db_name=self.tenant_info["alteryx_out_db_name"],
                                      tag_columns=['host','installation_id','ranger_type','ranger_id'],
                                      dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

    def add_timestamp_of_events(self, df):
        events = ['task_assigned', 'task_completed', 'reached_dock_point_s2p', 'going_pps_exit', 'reached_storable']
        for event in events:
            df[event + '_time'] = np.nan
            df[event + '_time'] = df.apply(lambda x: x['time'] if x['intermediate_event'] == event else np.nan, axis=1)
        group_df = df.groupby(['host', 'installation_id','ranger_type', 'ranger_id', 'task_key','ntile','subtask_key'], as_index=False).agg(
            first_task_assigned=('task_assigned_time', 'first'),
            last_task_completed=('task_completed_time', 'last'),
            first_reached_dock_point=('reached_dock_point_s2p_time', 'first'),
            last_going_pps_exit=('going_pps_exit_time', 'last'),
            first_reached_storable=('reached_storable_time', 'first'),
        )
        df = pd.merge(df, group_df, on=['host', 'installation_id','ranger_type', 'ranger_id', 'task_key','ntile','subtask_key'], how='left')
        return df

    def find_intermediate_time_differences(self, df):
        df['time_diff'] = 0
        time_diff_map = [
            ['first_task_assigned', 'last_task_completed', 'cycle_time'],
            ['first_reached_dock_point', 'last_going_pps_exit', 'relay_point_to_station'],
            ['last_going_pps_exit', 'last_task_completed', 'station_to_relay_point'],
            ['first_reached_storable', 'last_task_completed', 'storable_to_relay_point']
        ]
        for col in time_diff_map:
            start_event = col[0]
            end_event = col[1]
            new_event = col[2]
            df[new_event] = np.nan
            df = self.CommonFunction.datediff_in_millisec(df, end_event, start_event, new_event)
        return df

    def create_consolidated_events(self, df):
        temp_df = df[(df['intermediate_event'] == 'task_assigned')].reset_index(drop=True)
        events = ['cycle_time', 'relay_point_to_station', 'station_to_relay_point', 'storable_to_relay_point']
        selected_cols = ['time', 'event', 'host', 'installation_id', 'ranger_id', 'ranger_type', 'task_key',
                         'task_status','ntile','subtask_key','current_location_depth']
        consolidated = pd.DataFrame(columns=selected_cols + ['time_diff'])
        for event in events:
            temp_df['event'] = event
            temp_df['time_diff'] = temp_df[event]
            consolidated = pd.concat([consolidated, temp_df[selected_cols + ['time_diff']]])
        return consolidated

    def aggregate_similar_events(self, df):
        df = df.reset_index().groupby(['host', 'installation_id', 'ranger_type', 'ranger_id', 'task_key','ntile','subtask_key', 'event'],
                                      as_index=False).agg(
            task_status=('task_status', 'first'),
            time_diff=('time_diff', 'sum'),
            current_location_depth=('current_location_depth', 'first')
        )
        return df

    def add_completed_time_to_all_rows(self, df, completed_time):
        df = df.rename(columns={'time': 'event_time'})
        df = pd.merge(df, completed_time, on=['task_key','ntile','subtask_key'], how='left')
        df['time_diff'] = df['time_diff'].fillna(0)
        df = df[df['time_diff'] > 0].reset_index(drop=True)
        df['time'] = pd.to_datetime(df['time'])
        cols_to_del = ['ntile','index']
        for col in cols_to_del:
            if col in df.columns:
                del df[col]
        return df

    def consolidated_data_to_influx(self, df):
        relay_journey_consolidated = df.set_index('time')
        float_cols = ['time_diff','max_']
        relay_journey_consolidated = self.typecast_to_float(relay_journey_consolidated, float_cols)
        str_cols = [col for col in relay_journey_consolidated if col not in float_cols]
        relay_journey_consolidated = self.typecast_to_str(relay_journey_consolidated, str_cols)
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(relay_journey_consolidated, "relay_bot_journey",
                                      db_name=self.tenant_info["alteryx_out_db_name"],
                                      tag_columns=['host', 'installation_id', 'ranger_type', 'ranger_id', 'event'],
                                      dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
    
    def remove_out_of_sequence_events(self, df):
        ranger_event = df.copy(deep=True).reset_index()
        df = ranger_event.groupby(['host','installation_id','ranger_type','ranger_id','task_key','ntile','subtask_key','event'], as_index=False).agg(
            min_idx = ('index','min'),
            max_idx = ('index','max')
        ).reset_index(drop=True)
        df = pd.merge(ranger_event,df, on = ['host','installation_id','ranger_type','ranger_id','task_key','ntile','subtask_key','event'], how = 'left')
        max_list = ['reached_dock_point_p2s']
        both_min_max_list = ['reached_io_point_p2s']
        min_list = list(set(df['event'].unique().tolist()) - set(max_list) - set(both_min_max_list))
        condition1 = (df['event'].isin(max_list)) & (df['index'] == df['max_idx'])
        condition2 = (df['event'].isin(both_min_max_list)) & ((df['index'] == df['max_idx']) | (df['index'] == df['min_idx']))
        condition3 = (df['event'].isin(min_list)) & (df['index'] == df['min_idx'])
        df = df[(condition1 | condition2 | condition3)].reset_index(drop=True)  
        return df     

    def add_ntile(self, df):
        df = df.sort_values(by=['installation_id','ranger_type', 'ranger_id', 'task_key', 'time'],
                            ascending=[True, True, True, True, True]).reset_index()
        df['ntile'] = np.nan
        df['ntile'] = df.apply(lambda x: x['index'] if x['event']=='task_completed' else np.nan, axis=1)
        df['ntile'] = df['ntile'].fillna(method='bfill')
        df['ntile'] = df['ntile'].astype(int)
        if 'index' in df.columns:
            del df['index']
        return df 

    def final_call1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("relay_bot_journey", self.tenant_info)
        self.end_date = end_date.replace(second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}")

        ranger_events = self.fetch_raw_data()
        if not ranger_events.empty:
            ranger_events = self.get_time_diff_with_prev_event(ranger_events)
            self.write_raw_data_to_influx(ranger_events)

            ranger_events = self.add_ndeep(ranger_events)
            ranger_events = self.remove_unnecessary_events(ranger_events)            
            ranger_events = self.remove_duplicate_entries(ranger_events)
            ranger_events = self.add_ntile(ranger_events)
            ranger_events = self.remove_out_of_sequence_events(ranger_events)
            if not ranger_events.empty:
                ranger_events = self.create_metrics(ranger_events)
                self.write_metrics_to_influx(ranger_events)

                # for specific events this will add timestamp of events as a column
                final_df = self.add_timestamp_of_events(ranger_events)
                final_df = self.find_intermediate_time_differences(final_df)
                final_df = self.create_consolidated_events(final_df)
                completed_time = ranger_events[ranger_events['intermediate_event'] == 'task_completed'][['time', 'task_key','ntile','subtask_key']]
                ranger_events = self.aggregate_similar_events(ranger_events)
                ranger_events = pd.concat([ranger_events, final_df[ranger_events.columns]])
                ranger_events = self.add_completed_time_to_all_rows(ranger_events, completed_time)
                self.consolidated_data_to_influx(ranger_events)
            else:
                self.save_last_entry()
        else:
            self.save_last_entry()
        return None 

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
        'Relay_Bot_Journey',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Relay Bot Journey ',
        schedule_interval='*/15 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(minutes=60)
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['relay_bot_journey'] == "Y" and (
                    tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                relay_bot_journey_final_task = PythonOperator(
                    task_id='relay_bot_journey_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(RelayBotJourney().final_call,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        relay_bot_journey_final_task = PythonOperator(
            task_id='relay_bot_journey_final',
            provide_context=True,
            python_callable=functools.partial(RelayBotJourney().final_call, tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )