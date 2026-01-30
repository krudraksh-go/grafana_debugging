## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import pytz
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
from r2r_waittime_calculation import R2rWaittimeCalculation
from operator_working_time_summary import operator_working_time_summary
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------
import numpy as np

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class ml_vs_predicted:
    def number_of_loads(self, df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['event'][x - 1] == 'going_to_entity_location') and
                        (df['event'][x] == 'entity_loading_started' and
                         (df['ranger_id'][x] == df['ranger_id'][x - 1]) and (df['installation_id'][x] == df['installation_id'][x - 1]))):
                    df['load_on_ranger'][x] = df['load_on_ranger'][x - 1]
                elif (((df['event'][x - 1] == 'entity_loading_started') and (
                        df['load_event'][x - 1] == 'entity_loaded')) and
                      (df['event'][x] == 'entity_loading_started' and (df['installation_id'][x] == df['installation_id'][x - 1]) and
                       (df['ranger_id'][x] == df['ranger_id'][x - 1]))):
                    df['load_on_ranger'][x] = df['load_on_ranger'][x - 1] + 1

        return df

    def total_time_calc3(self, df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['event'][x - 1] == 'entity_loading_started') and (df['load_event'][x] == 'entity_loaded') and (
                        df['tote_count_ndeep'][x - 1] == 1) and
                        (df['load_event'][x - 1] == 'entity_loading_started' and
                         (df['ranger_id'][x] == df['ranger_id'][x - 1]) and (df['installation_id'][x] == df['installation_id'][x - 1]))):
                    df['total_time'][x - 1] = df['total_time'][x] + df['total_time'][x - 1]

        return df

    def total_time_calc(self, df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['event'][x - 1] == 'going_to_entity_location') and
                        (df['event'][x] == 'entity_loading_started' and  (df['ndeep'][x] == df['ndeep'][x-1]) and
                        (df['ranger_id'][x] == df['ranger_id'][x-1]) and (df['installation_id'][x] == df['installation_id'][x - 1]))):
                    df['total_time'][x - 1] = df['total_time'][x]

        return df

    def picking_at_same_location(self, df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['event'][x - 1] == df['event'][x]) and
                        df['event'][x] == 'entity_loading_started' and
                        df['current_location'][x] == df['current_location'][x - 1] and
                        df['ranger_id'][x] == df['ranger_id'][x - 1] and (df['installation_id'][x] == df['installation_id'][x - 1])):
                    df['picking_at_same_pos'][x] = 1

        return df

    def total_time_calc2(self, df):
        for x, row in df.iloc[::-1].iterrows():
            # print(f"Index: {index}, Name: {row['Name']}, Score: {row['Score']}")
            if x > 0:
                if ((df['event'][x - 1] == 'going_to_entity_location') and
                        (df['event'][x] == 'going_to_entity_location' and df['entity_id'][x] == df['entity_id'][x - 1] and
                         df['ndeep'][x] == df['ndeep'][x - 1] and df['tote_count_ndeep'][x] == df['tote_count_ndeep'][x - 1] and
                         (df['ranger_id'][x] == df['ranger_id'][x - 1]) and (df['installation_id'][x] == df['installation_id'][x - 1]))):
                    df['total_time'][x - 1] = df['total_time'][x]

        return df
    
    def ml_vs_predicted_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        #        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("ml_vs_predicted", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from ranger_task_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.ml_vs_predicted_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("ml_vs_predicted", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("ml_vs_predicted", '5min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.ml_vs_predicted_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def number_of_unloads(self,df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['event'][x - 1] == 'going_to_destination') and
                        (df['event'][x] == 'entity_unloading_started' and
                         (df['ranger_id'][x] == df['ranger_id'][x - 1]) and (df['installation_id'][x] == df['installation_id'][x - 1]) )):
                    df['load_on_ranger'][x] = df['load_on_ranger'][x - 1]
                elif (((df['event'][x - 1] == 'entity_unloading_started') and (
                        df['load_event'][x - 1] == 'entity_unloaded')) and
                      (df['event'][x] == 'entity_unloading_started' and
                       (df['ranger_id'][x] == df['ranger_id'][x - 1]) and (df['installation_id'][x] == df['installation_id'][x - 1]))):
                    df['load_on_ranger'][x] = df['load_on_ranger'][x - 1] - 1

        return df

    def total_time_calc3_unload(self, df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['event'][x - 1] == 'entity_unloading_started') and (
                        df['load_event'][x] == 'entity_unloaded') and (df['tote_count_ndeep'][x - 1] == 1) and
                        (df['load_event'][x - 1] == 'entity_unloading_started' and
                         (df['ranger_id'][x] == df['ranger_id'][x - 1]) and (df['installation_id'][x] == df['installation_id'][x - 1]))):
                    df['total_time'][x - 1] = df['total_time'][x] + df['total_time'][x - 1]

        return df

    def total_time_calc_unload(self, df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['event'][x - 1] == 'going_to_destination') and
                        (df['event'][x] == 'entity_unloading_started' and  (df['ndeep'][x] == df['ndeep'][x-1]) and
                         (df['ranger_id'][x - 1] == df['ranger_id'][x]) and (df['installation_id'][x] == df['installation_id'][x - 1]) )):
                    df['total_time'][x - 1] = df['total_time'][x]

        return df

    def total_time_calc2_unload(self, df):
        for x, row in df.iloc[::-1].iterrows():
            # print(f"Index: {index}, Name: {row['Name']}, Score: {row['Score']}")
            if x > 0:
                if ((df['event'][x - 1] == 'going_to_destination') and
                        (df['event'][x] == 'going_to_destination' and (df['entity_id'][x] == df['entity_id'][x - 1]) and
                         df['ndeep'][x] == df['ndeep'][x - 1] and df['tote_count_ndeep'][x] == df['tote_count_ndeep'][x - 1] and
                         (df['ranger_id'][x] == df['ranger_id'][x - 1]) and (df['installation_id'][x] == df['installation_id'][x - 1]))):
                    df['total_time'][x - 1] = df['total_time'][x]

        return df

    def picking_at_same_location_unload(self,df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['event'][x - 1] == df['event'][x]) and
                        df['event'][x] == 'entity_unloading_started' and
                        df['current_location'][x] == df['current_location'][x - 1] and
                        df['ranger_id'][x] == df['ranger_id'][x - 1]) and (df['installation_id'][x] == df['installation_id'][x - 1]):
                    df['picking_at_same_pos'][x] = 1

        return df

    def date_compare(self, x):
        diff = (x['start'].tz_localize(tz=pytz.UTC) - x['time'].tz_convert(tz=pytz.UTC)).total_seconds()
        # diff = (x['start'].tz_localize(tz=pytz.UTC)-x['interval_start'].tz_localize(tz=pytz.UTC)).total_seconds()
        if diff <= 0:
            return True
        return False

    def load_df(self,df):
        if not df.empty:
            df = df.sort_values(by=['installation_id','ranger_id', 'time'], ascending=[False, False, True])
            df = df.reset_index()
            if 'level_0' in df.columns:
                del df['level_0']
            if 'index' in df.columns:
                del df['index']

            if not df.empty:
                l = ['current_location_type', 'entity_type', 'event_reason', 'fulfilment_area', 'host', 'journey_type',
                     'next_destination', 'next_destination_type', 'ranger_version', 'task_key', 'task_status',
                     'task_type'
                    , 'bot_max_height', 'bot_fork_height', 'journey_destination']
                for col in l:
                    if col in df.columns:
                        del df[col]
            df['load_time'] = df['time'].shift(-1)
            df['load_event'] = df['event'].shift(-1)
            df['load_ranger_id'] = df['ranger_id'].shift(-1)
            df['tote_load_count'] = df['load_on_ranger'].shift(-1)
            df['load_installation_id'] = df['installation_id'].shift(-1)


            df['total_time'] = np.nan
            df = self.utilfunction.datediff_in_millisec(df, 'load_time', 'time', 'total_time')
            df['total_time'] = df.apply(lambda x: x['total_time'] if x['load_ranger_id'] == x['ranger_id'] and  x['load_installation_id'] == x['installation_id'] and (
                        (x['load_event'] == 'entity_loaded' and x['event'] == 'entity_loading_started') or (
                            x['load_event'] == 'entity_loading_started' and x[
                        'event'] == 'entity_loading_started')) else 0, axis=1)

            df = df[((df['total_time'] != 0) | (df['event'] == 'going_to_entity_location'))]

            for col in ['tray_numbers']:
                split_cols = df[col].str.split(',', expand=True)
                split_cols.columns = [f"{col}_{i}" for i in range(split_cols.shape[1])]
                df = df.join(split_cols)

            df[col] = df[f"{col}_0"]
            del df[f"{col}_0"]
            if f"{col}_1" in df.columns:
                del df[f"{col}_1"]

            df = df[~pd.isna(df['calculated_load_unload_time'])]
            if not df.empty:
                df = df.reset_index()
                if 'level_0' in df.columns:
                    del df['level_0']
                if 'index' in df.columns:
                    del df['index']
                df['picking_at_same_pos'] = 0
                # df['updated_flag']=0
                # df['prev_fork_height_value']=0
                df = self.number_of_loads(df)
                df = self.total_time_calc3(df)
                df = self.total_time_calc(df)
                df = self.picking_at_same_location(df)
                df = self.total_time_calc2(df)
                # df = support_ndeep(df)
                df['load_on_ranger'] = df.apply(
                    lambda x: int(x['tote_load_count']) - 1 if x['event'] == 'entity_loading_started' and not pd.isna(
                        x['tote_load_count']) else x['load_on_ranger'], axis=1)

                df['event_numeric'] = df['event'].astype('category').cat.codes
                df['is_max_storage_limit_exceed'] = df.apply(
                    lambda x: 1 if x['storage_shelf_height'] > x['max_bot_height'] else 0, axis=1)
                df['is_max_fork_limit_exceed'] = df.apply(
                    lambda x: 1 if x['fork_height_value'] > x['max_bot_height'] else 0, axis=1)
                # df['is_max_fork_limit_exceed_ndeep']= df.apply(lambda x:1 if x['prev_fork_height_value']> x['max_bot_height'] else 0 ,axis=1)

                df['tray_numbers'] = df['tray_numbers'].apply(lambda x: x.replace('TRAY_', '') if not pd.isna(x) else 0)
                # if 'tray_numbers_1' in df.columns:
                #     df['tray_numbers_1']=df['tray_numbers_1'].apply(lambda x:x.replace('TRAY_','')  if not pd.isna(x) else 0)

                df['tray_numbers_height'] = df.apply(lambda x: x['max_bot_height'] * int(x['tray_numbers']) / 8, axis=1)
                # if 'tray_numbers_1' in df.columns:
                #     df['tray_numbers_height_ndeep']=df.apply(lambda x:x['max_bot_height']*int(x['tray_numbers_1'])/8,axis=1)
                # df['tray_numbers_height']=df.apply(lambda x:int(x['tray_numbers'])*10.0,axis=1)
                df['idc_time'] = df['idc_time'].apply(lambda x: x if not pd.isna(x) else 0)
                df = df[(df['total_time'] != 0)]
                if not df.empty:
                    df['load_on_ranger'] = df['load_on_ranger'].fillna(0)
                    df['tote_count_ndeep'] = df['tote_count_ndeep'].fillna(0)
                    df['height_diff'] = df.apply(lambda x: abs(x['storage_shelf_height'] - x['fork_height_value']), axis=1)
                    df['storage_tray_diff'] = df.apply(lambda x: abs(x['storage_shelf_height'] - x['tray_numbers_height']),
                                                       axis=1)

                    if not df.empty:
                        l = ['load_installation_id','entity_id','storage_location', 'load_ranger_id', 'load_event', 'load_time',
                             'ranger_id', 'max_bot_height', 'idc', 'calculated_time_to_load_unload', 'shelf_height',
                             'tray_numbers', 'current_location', 'tote_load_count', 'tray_numbers_1', 'updated_flag', 'ndeep_1']
                        for col in l:
                            if col in df.columns:
                                del df[col]
            if df.empty:
                df = pd.DataFrame()
        return df

    def unload_df(self, df):
        if not df.empty:
            l = ['current_location_type', 'entity_type', 'event_reason', 'fulfilment_area', 'host',
                 'journey_type', 'next_destination', 'next_destination_type', 'ranger_version', 'task_key',
                 'task_status', 'task_type'
                , 'bot_max_height', 'bot_fork_height', 'journey_destination']
            for col in l:
                if col in df.columns:
                    del df[col]
            df = df.sort_values(by=['installation_id','ranger_id', 'time'], ascending=[False, False, True])
            df = df.reset_index()
            if 'level_0' in df.columns:
                del df['level_0']
            if 'index' in df.columns:
                del df['index']
            df['load_time'] = df['time'].shift(-1)
            df['load_event'] = df['event'].shift(-1)
            df['load_ranger_id'] = df['ranger_id'].shift(-1)
            df['tote_load_count'] = df['load_on_ranger'].shift(-1)
            df['prev_current_location'] = df['current_location'].shift(-1)
            df['load_installation_id'] = df['installation_id'].shift(-1)
            df = df[~((df['current_location'] == df['prev_current_location']) & (df['event'] == df['load_event']) & (
                        df['event'] == 'going_to_destination'))]
            df = df.reset_index()
            if 'level_0' in df.columns:
                del df['level_0']
            if 'index' in df.columns:
                del df['index']
            del df['prev_current_location']
            df['total_time'] = np.nan
            df = self.utilfunction.datediff_in_millisec(df, 'load_time', 'time', 'total_time')
            df['total_time'] = df.apply(lambda x: x['total_time'] if x['load_ranger_id'] == x['ranger_id'] and x['load_installation_id'] == x['installation_id'] and (
                        (x['load_event'] == 'entity_unloaded' and x['event'] == 'entity_unloading_started') or (
                            x['load_event'] == 'entity_unloading_started' and x[
                        'event'] == 'entity_unloading_started')) else 0, axis=1)
            df = df[((df['total_time'] != 0) | (df['event'] == 'going_to_destination'))]
            df = df[~pd.isna(df['calculated_load_unload_time'])]
            df = df.reset_index()
            if not df.empty:
                if 'level_0' in df.columns:
                    del df['level_0']
                if 'index' in df.columns:
                    del df['index']
                df['picking_at_same_pos'] = 0
                df = self.number_of_unloads(df)
                df = self.total_time_calc3_unload(df)
                df = self.total_time_calc_unload(df)
                df = self.picking_at_same_location_unload(df)
                df = self.total_time_calc2_unload(df)
                df['load_on_ranger'] = df.apply(
                    lambda x: int(x['tote_load_count']) - 1 if x['event'] == 'entity_unloading_started' and not pd.isna(
                        x['tote_load_count']) else x['load_on_ranger'], axis=1)
                df['event_numeric'] = df['event'].astype('category').cat.codes
                df['is_max_storage_limit_exceed'] = df.apply(
                    lambda x: 1 if x['storage_shelf_height'] > x['max_bot_height'] else 0, axis=1)
                df['is_max_fork_limit_exceed'] = df.apply(
                    lambda x: 1 if x['fork_height_value'] > x['max_bot_height'] else 0, axis=1)
                df['tray_numbers'] = df['tray_numbers'].apply(lambda x: x.replace('TRAY_', '') if not pd.isna(x) else '0')
                df['tray_numbers'] = df['tray_numbers'].apply(lambda x: '-1' if x == 'FORK' else x)
                df['tray_numbers'] = df['tray_numbers'].apply(lambda x: x.replace('FORK', '0') if not pd.isna(x) else 0)
                df['tray_numbers_height'] = df.apply(lambda x: x['max_bot_height'] * int(x['tray_numbers']) / 8, axis=1)
                df['tray_numbers_height']
                df['tray_numbers_height'] = df.apply(
                    lambda x: x['fork_height_value'] if x['tray_numbers_height'] < 0 else x['tray_numbers_height'], axis=1)
                df['tray_numbers_height']
                # df['tray_numbers_height']=df.apply(lambda x:int(x['tray_numbers'])*10.0,axis=1)
                if not df.empty:
                    l = [ 'load_installation_id', 'entity_id',  'storage_location', 'load_ranger_id',
                         'load_event', 'load_time', 'ranger_id', 'max_bot_height', 'idc', 'calculated_time_to_load_unload',
                         'shelf_height', 'ndeep_tote_count', 'tray_numbers', 'current_location', 'tote_load_count',
                         'ndeep_1']
                    for col in l:
                        if col in df.columns:
                            del df[col]
                df['idc_time'] = df['idc_time'].apply(lambda x: x if not pd.isna(x) else 0)
                df = df[(df['total_time'] != 0)]
                if not df.empty:
                        df['load_on_ranger'] = df['load_on_ranger'].fillna(0)
                        df['height_diff'] = df.apply(lambda x: abs(x['tray_numbers_height'] - x['fork_height_value']), axis=1)
                        df['storage_tray_diff'] = df.apply(lambda x: abs(x['storage_shelf_height'] - x['tray_numbers_height']),
                                                           axis=1)
            if df.empty:
                df=pd.DataFrame()
            # df['telscope_pos']=df.apply(lambda x: 0 if int(x['is_max_storage_limit_exceed'])==int(x['is_max_fork_limit_exceed']) else 1 ,axis=1)
            # df['height_diff_with_telscropt']=df.apply(lambda x: x['height_diff'] if int(x['telscope_pos'])==0 else x['height_diff']+5000 ,axis=1)
            return df

    def ml_vs_predicted_final1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("ml_vs_predicted", self.tenant_info)
        temp_data = pd.to_datetime(self.start_date) - timedelta(minutes=10)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()
        print(self.start_date)

        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        #q = f"select installation_id,pps_id,uom_quantity_int,slotref,value,item_id,order_id,uom_quantity,station_type,storage_type,num_of_orderlines,pps_point from item_picked where time>'{temp_data}' and time <= '{self.end_date}' and value > 0 order by time desc"
        #q = "select * from ranger_task_events where time>'{temp_data}' and time <= '{self.end_date}'   and (event='entity_unloading_started' or event ='entity_unloaded'or event='going_to_destination') order by time desc"
        q = f"select * from ranger_task_events where time>'{temp_data}' and time <= '{self.end_date}'   and (event='entity_loading_started' or event ='entity_loaded'or event='going_to_entity_location') order by time desc"
        q1 = f"select * from ranger_task_events where time>'{temp_data}' and time <= '{self.end_date}'   and (event='entity_unloading_started' or event ='entity_unloaded'or event='going_to_destination') order by time desc"
        # q ="select * from ranger_task_events where time >now()-14h  and (event='entity_loading_started' or event ='entity_loaded'or event='going_to_entity_location')  order by time desc"
        # q1 = "select * from ranger_task_events where  time >now()-14h  and (event='entity_unloading_started' or event ='entity_unloaded'or event='going_to_destination') order by time desc"
        print(q)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        df1 = pd.DataFrame(self.read_client.query(q1).get_points())
        # print(df.shape)
        # print(df1.shape)
        if df.empty and df1.empty:
            filter = {"site": self.tenant_info['Name'], "table": "ml_vs_predicted"}
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter, new_last_run)
        else:
            df =self.load_df(df)
            df1 = self.unload_df(df1)


        # print(df.columns)
        # print(df1.columns)
        # df.to_csv(r'/home/ankush.j/dev/gm_data_flow/dags/load_df_new.csv', index=False, header=True)
        # df1.to_csv(r'/home/ankush.j/dev/gm_data_flow/dags/unload_df1_new.csv', index=False, header=True)
        combine_df = pd.concat([df, df1])
        if not combine_df.empty:
            combine_df.time = pd.to_datetime(combine_df.time)
            combine_df["start"] = pd.to_datetime(self.start_date) - timedelta(minutes=5)
            combine_df["flag2"] = combine_df.apply(self.date_compare, axis=1)
            combine_df = combine_df[(combine_df["flag2"])]
            combine_df['fork_height_value'] = combine_df['fork_height_value'].astype(float)
            combine_df['calculated_load_unload_time'] = combine_df['calculated_load_unload_time'].astype(float)
            combine_df['event_numeric'] = combine_df['event_numeric'].astype(int)
            combine_df['fork_height_value'] = combine_df['fork_height_value'].astype(float)
            combine_df['height_diff'] = combine_df['height_diff'].astype(float)
            combine_df['idc_time'] = combine_df['idc_time'].astype(float)
            combine_df['is_max_fork_limit_exceed'] = combine_df['is_max_fork_limit_exceed'].astype(int)
            combine_df['is_max_storage_limit_exceed'] = combine_df['is_max_storage_limit_exceed'].astype(int)
            combine_df['load_on_ranger'] = combine_df['load_on_ranger'].astype(float)
            combine_df['picking_at_same_pos'] = combine_df['picking_at_same_pos'].astype(int)
            combine_df['storage_shelf_height'] = combine_df['storage_shelf_height'].astype(float)
            combine_df['storage_tray_diff'] = combine_df['storage_tray_diff'].astype(float)
            combine_df['total_time'] = combine_df['total_time'].astype(float)
            combine_df['tote_count_ndeep'] = combine_df['tote_count_ndeep'].astype(float)
            combine_df['tray_numbers_height'] = combine_df['tray_numbers_height'].astype(float)

            if not combine_df.empty:
                combine_df = combine_df.drop(['flag2', 'start'], axis=1)
            combine_df.time = pd.to_datetime(combine_df.time)
            combine_df = combine_df.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(combine_df, "ml_vs_predicted", db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['event'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None


with DAG(
        'ml_vs_predicted',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='ml_vs_predicted',
        schedule_interval = '29 * * * *',
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
            if tenant['Active'] == "Y" and tenant['ml_vs_predicted'] == "Y":
                try:
                    final_task = PythonOperator(
                        task_id='ml_vs_predicted_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(ml_vs_predicted().ml_vs_predicted_final,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=3600),
                    )
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='ml_vs_predicted_final',
            provide_context=True,
            python_callable=functools.partial(ml_vs_predicted().ml_vs_predicted_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

