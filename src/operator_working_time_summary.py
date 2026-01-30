## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------
#
import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
import pytz
import math
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, MongoDBManager
from pandasql import sqldf
from config import (
    MongoDbServer
)
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------
import warnings

warnings.filterwarnings("ignore")

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.8.1.244')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.8.1.244')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name = os.environ.get('Out_db_name', 'GreyOrange')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class operator_working_time_summary:

    def apply_transaction_group(self, df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['event_name'][x] == 'transaction_start') or
                        (df['pps_id'][x] != df['pps_id'][x - 1]) or
                        (df['installation_id'][x] != df['installation_id'][x - 1])):
                    group = group + 1
                df['transaction_group'][x] = group
        return df

    def apply_transaction_group2(self, df):
        group = 0
        for x in df.index:
            if x >= 0:
                if df['butler_movement'][x]=='arrival':
                    group = group + 1
                df['transaction_group'][x] = group

                # if df['event_name']!='wait_for_msu_start':
                #     if ((df['rack_id'][x] != df['rack_id'][x-1] ) or
                #             (df['pps_id'][x] != df['pps_id'][x - 1]) or
                #             (df['installation_id'][x] != df['installation_id'][x - 1])):
                #         group = group + 1

        return df



    def operator_working_time_summary(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["write_influx_ip"],
                                      port=self.tenant_info["write_influx_port"], db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("operator_working_time_summary_airflow", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        # self.remove_unnecassary_tag_keys()

        q = f"select *  from flow_transaction_events where time>'{check_start_date}' and time<='{check_end_date}' and pps_mode ='pick_mode' limit 1"
        df_flow_transaction_events = pd.DataFrame(self.read_client.query(q).get_points())
        if df_flow_transaction_events.empty:
            self.end_date = datetime.now(timezone.utc)
            self.operator_working_time_summary1(self.end_date,df_flow_transaction_events.empty, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("operator_working_time_summary_airflow", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("operator_working_time_summary_airflow", '5min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.operator_working_time_summary1(self.end_date,df_flow_transaction_events.empty, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def apply_ntile(self, df):
        Group = 1
        for x in df.index:
            if x > 0:
                if df['installation_id'][x] == df['installation_id'][x - 1] and df['pps_id'][x] == df['pps_id'][
                    x - 1] and df['rack_id'][x] == df['rack_id'][x - 1]:
                    df["ntile"][x] = df["ntile"][x - 1]
                else:
                    Group = Group + 1
                    df["ntile"][x] = Group
            else:
                df["ntile"][x] = Group
        return df

    def date_compare(self, x):
        diff = (x['start'].tz_convert(tz=pytz.UTC) - x['time'].tz_convert(tz=pytz.UTC)).total_seconds()
        # diff = (x['start'].tz_localize(tz=pytz.UTC)-x['interval_start'].tz_localize(tz=pytz.UTC)).total_seconds()
        if diff <= 0:
            return True
        return False

    def operator_working_time_summary1(self, end_date,emptyflag, **kwargs):
        self.start_date = self.client.get_start_date("operator_working_time_summary_airflow", self.tenant_info)
        self.utilfunction = CommonFunction()
        temp_data= pd.to_datetime(self.start_date) - timedelta(minutes=60)
        self.start_date = pd.to_datetime(self.start_date)
        print(f"start date: {temp_data}, end_date : {self.end_date}")
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        temp_data = temp_data.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        utilfunction = CommonFunction()
        if self.tenant_info['Name'] !='DHL_Figs':
            q = f"select * from flow_transaction_events where time>'{self.start_date}' and time<='{self.end_date}'  and event_name='pptl_event_processed' and pps_mode ='pick' limit 1"
            df_main = pd.DataFrame(self.read_client.query(q).get_points())
            if df_main.empty:
                filter = {"site": self.tenant_info['Name'], "table": "operator_working_time_summary_airflow"}
                new_last_run = {"last_run": self.end_date}
                utilfunction.update_dag_last_run(filter, new_last_run)
                return

            q = f"select * from flow_transaction_events where time>'{temp_data}' and time<='{self.end_date}' and (((event_name='pptl_event_processed' or event_name='transaction_start' or event_name = 'safety_pause_start' or event_name = 'safety_pause_end') and pps_mode ='pick') or (butler_movement='arrival' and pps_mode='pick_mode'))  order by time"
            df_main = pd.DataFrame(self.read_client.query(q).get_points())
            if not df_main.empty and 'carrier_type' in df_main.columns:
                df_main = df_main[df_main['carrier_type'] != 'packing_box']

            if not df_main.empty:
                if 'uom' not in df_main.columns:
                    df_main['uom'] = 'undefined'

                if 'pps_point' not in df_main.columns:
                    df_main['pps_point'] = 'undefined'

                df_main = df_main.sort_values(by=['installation_id', 'pps_id', 'time'], ascending=[True, True, True])
                df_main = utilfunction.reset_index(df_main)

                ttp_setup = (self.tenant_info['is_ttp_setup'] == 'Y')
                df_main = utilfunction.update_station_type(df_main, ttp_setup)
                df_main = utilfunction.update_storage_type(df_main, ttp_setup)

                df_main['prev_installation_id'] = df_main['installation_id'].shift(1)
                df_main['prev_pps_id'] = df_main['pps_id'].shift(1)
                df_main['prev_event_name'] = df_main['event_name'].shift(1)
                df_main['prev_rack_id'] = df_main['rack_id'].shift(1)
                df_main = df_main[((df_main['event_name'] != df_main['prev_event_name']) | (
                                           df_main['pps_id'] != df_main['prev_pps_id']) | (
                                           df_main['installation_id'] != df_main['prev_installation_id']) )]
                if not df_main.empty:
                    l = ['prev_installation_id', 'prev_pps_id', 'prev_event_name', 'prev_rack_id']
                    for col in l:
                        if col in df_main.columns:
                            del df_main[col]
                df_main['time'] = pd.to_datetime(df_main['time'], utc=True)
                if 'operator_id' not in df_main.columns:
                    df_main['operator_id'] = "default"
                df_main['breach_start_time'] = np.nan
                df_main['breach_end_time'] = np.nan                    
                df_main['transaction_start_time'] = np.nan
                df_main['transaction_end_time'] = np.nan
                df_main['pptl_event_processed_time'] = np.nan
                df_main['breach_start_time'] = df_main.apply(
                    lambda x: x['time'] if x['event_name'] == 'safety_pause_start' else np.nan, axis=1)
                df_main['breach_end_time'] = df_main.apply(
                    lambda x: x['time'] if x['event_name'] == 'safety_pause_end' else np.nan, axis=1)                
                df_main['transaction_start_time'] = df_main.apply(
                    lambda x: x['time'] if x['event_name'] == 'transaction_start' else np.nan, axis=1)
                df_main['transaction_end_time'] = df_main.apply(
                    lambda x: x['time'] if x['event_name'] == 'transaction_end' else np.nan, axis=1)
                df_main['pptl_event_processed_time'] = df_main.apply(
                    lambda x: x['time'] if (x['event_name'] == 'pptl_event_processed' and x[
                        'event_type'] == 'product_placed_in_bin') else np.nan, axis=1)
                df_main['pps_mode'] = df_main['pps_mode'].apply(
                    lambda x: x.replace('_mode', '') if not pd.isna(x) and len(x) > 4 else x)
                if 'butler_movement' in df_main.columns:
                    df_main['arrival_time'] = df_main.apply(
                        lambda x: x['time'] if x['butler_movement'] == 'arrival' else np.nan, axis=1)

                df_main['transaction_group'] = 0

                df_main = utilfunction.reset_index(df_main)
                df_main = self.apply_transaction_group(df_main)
                # df_main.to_csv(r'/home/ankush.j/Desktop/workname/big_data/df_main1.csv', index=False, header=True)
                df_main = df_main.sort_values(
                    by=['installation_id', 'pps_id', 'time', 'rack_id'],
                    ascending=[True, True, True, True])
                df_main = df_main.groupby(
                    ['installation_id', 'pps_id', 'pps_mode', 'transaction_group'],
                    as_index=False).agg(
                    time=('time', 'min'),
                    rack_id=('rack_id', 'first'),
                    storage_type = ('storage_type', 'first'),
                    station_type = ('station_type', 'first'),
                    operator_id=('operator_id', 'first'),
                    first_transaction_start_time=('transaction_start_time', 'min'),
                    pptl_event_processed_time=('pptl_event_processed_time', 'max'),
                    next_rack_arrival_time=('arrival_time', 'max'),
                    time_end=('time', 'max'),
                    breach_start = ('breach_start_time','min'),
                    breach_end = ('breach_end_time','max'))
                df_main = df_main[(~pd.isna(df_main['rack_id']))]
                df_main = df_main[~(pd.isna(df_main['pptl_event_processed_time']))]
                df_main = df_main[~(pd.isna(df_main['first_transaction_start_time']))]

                if df_main.empty:
                    return
                df_main['breach_start'] = df_main.apply(lambda x : max(x['breach_start'], x['pptl_event_processed_time']) if pd.notna(x['pptl_event_processed_time']) and pd.notna(x['breach_start']) else x['breach_start'], axis = 1)
                df_main = df_main.sort_values(
                    by=['installation_id', 'pps_id', 'pps_mode', 'transaction_group', 'time', 'rack_id'],
                    ascending=[True, True, True, True, True, True])
                df_main = utilfunction.reset_index(df_main)
                df_main['first_transaction_start_time_upper'] = df_main['first_transaction_start_time'].shift(-1)
                df_main['pps_id_upper'] = df_main['pps_id'].shift(-1)
                df_main['installation_id_upper'] = df_main['installation_id'].shift(-1)
                df_main['first_transaction_start_time_upper'] = df_main.apply(lambda x:x['first_transaction_start_time_upper'] if x['installation_id']==x['installation_id_upper'] and x['pps_id']==x['pps_id_upper'] else np.nan, axis=1)
                df_main['opertor_working_time'] = np.nan
                df_main['breach_time'] = np.nan  
                df_main = utilfunction.datediff_in_millisec(df_main, 'breach_end',
                                                                'breach_start',
                                                                'breach_time')
                                                                                            
                df_main = utilfunction.datediff_in_millisec(df_main, 'pptl_event_processed_time',
                                                            'first_transaction_start_time',
                                                            'opertor_working_time')
                df_main['operator_waiting_time'] = np.nan

                df_main = utilfunction.datediff_in_millisec(df_main, 'first_transaction_start_time_upper',
                                                            'pptl_event_processed_time',
                                                            'operator_waiting_time')
                df_main = utilfunction.datediff_in_millisec(df_main, 'time_end',
                                                                'pptl_event_processed_time',
                                                                'operator_waiting_time')
                df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = np.nan
                df_main = utilfunction.datediff_in_millisec(df_main, 'next_rack_arrival_time',
                                                            'pptl_event_processed_time',
                                                            'time_diff_b_w_pptl_press_to_rack_arrival')
                df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = df_main['time_diff_b_w_pptl_press_to_rack_arrival'].apply(lambda x: 0 if x <= 0 or pd.isna(x) else x)
                df_main['breach_time'] = df_main['breach_time'].fillna(0)
                
                df_main["ntile"] = 0
                df_main=self.apply_ntile(df_main)
                df_main = df_main.groupby(
                    ['installation_id', 'pps_id', 'pps_mode', 'ntile'],
                    as_index=False).agg(
                    time=('time', 'min'),
                    rack_id=('rack_id', 'first'),
                    storage_type = ('storage_type', 'first'),
                    station_type = ('station_type', 'first'),
                    operator_id=('operator_id', 'first'),
                    first_transaction_start_time=('first_transaction_start_time','min'),
                    pptl_event_processed_time=('pptl_event_processed_time', 'max'),
                    next_rack_arrival_time=('next_rack_arrival_time', 'max'),
                    operator_waiting_time=('operator_waiting_time', 'sum'),
                    opertor_working_time=('opertor_working_time', 'sum'),
                    time_diff_b_w_pptl_press_to_rack_arrival=('time_diff_b_w_pptl_press_to_rack_arrival', 'sum'),
                    breach_time = ('breach_time','sum'),)

                df_main['operator_waiting_time'] = df_main['operator_waiting_time'].astype(int, errors='ignore')
                df_main['opertor_working_time'] = df_main['opertor_working_time'].astype(int, errors='ignore')
                df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = df_main['time_diff_b_w_pptl_press_to_rack_arrival'].astype(int, errors='ignore')
                df_main['pps_id'] = df_main['pps_id'].astype(str, errors='ignore')
                df_main['rack_id'] = df_main['rack_id'].astype(str, errors='ignore')
                df_main['operator_id'] = df_main['operator_id'].astype(str, errors='ignore')


                df_main["start"] = pd.to_datetime(self.start_date) - timedelta(minutes=30)
                df_main["flag2"] = df_main.apply(self.date_compare, axis=1)
                df_main = df_main[(df_main["flag2"])]
                if not df_main.empty:
                    df_main = df_main.drop(['flag2', 'start'], axis=1)

                if not df_main.empty and not emptyflag:
                    l = ['first_transaction_start_time_upper', 'pps_id_upper', 'installation_id_upper', 'prev_rack_id','transaction_group','time_end','index','ntile','start']
                    for col in l:
                        if col in df_main.columns:
                            del df_main[col]
                    df_main[['opertor_working_time', 'operator_waiting_time', 'time_diff_b_w_pptl_press_to_rack_arrival']] = df_main[['opertor_working_time', 'operator_waiting_time', 'time_diff_b_w_pptl_press_to_rack_arrival']].fillna(0)
                    df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = df_main.apply(lambda x : x['time_diff_b_w_pptl_press_to_rack_arrival'] - x['breach_time'] if (x['time_diff_b_w_pptl_press_to_rack_arrival'] - x['breach_time'])>0 else 0, axis = 1)
                    df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = df_main['time_diff_b_w_pptl_press_to_rack_arrival'].astype(int)
                    df_main['breach_time'] = df_main['breach_time'].astype(int)

                    df_main['Actual_operator_working_time']=df_main.apply(lambda x: x['opertor_working_time']+x['operator_waiting_time']-x['time_diff_b_w_pptl_press_to_rack_arrival'],axis=1)
                    df_main['Actual_operator_working_time'] = df_main['Actual_operator_working_time'].apply(lambda x: 0 if x <= 0 or pd.isna(x) else x)
                    df_main.first_transaction_start_time = df_main.first_transaction_start_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(x) else x)
                    df_main.pptl_event_processed_time = df_main.pptl_event_processed_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(x) else x)
                    df_main.next_rack_arrival_time = df_main.next_rack_arrival_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(x) else x)
                    df_main['first_transaction_start_time'] = df_main['first_transaction_start_time'].astype(str, errors='ignore')
                    df_main['pptl_event_processed_time'] = df_main['pptl_event_processed_time'].astype(str, errors='ignore')
                    df_main['next_rack_arrival_time'] = df_main['next_rack_arrival_time'].astype(str, errors='ignore')


                    df_main.time = pd.to_datetime(df_main.time)
                    df_main = df_main.set_index('time')
                    self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                         port=self.tenant_info["write_influx_port"])
                    self.write_client.writepoints(df_main, "operator_working_time_summary_airflow",
                                                  db_name=self.tenant_info["alteryx_out_db_name"],
                                                  tag_columns=['pps_id'],
                                                  dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                else:
                    filter = {"site": self.tenant_info['Name'], "table": "operator_working_time_summary_airflow"}
                    new_last_run = {"last_run": self.end_date}
                    self.utilfunction.update_dag_last_run(filter, new_last_run)
        else:
            q = f"select * from flow_transaction_events where time>'{self.start_date}' and time<='{self.end_date}'   and pps_mode ='pick_mode' limit 1"
            df_main = pd.DataFrame(self.read_client.query(q).get_points())
            if df_main.empty:
                filter = {"site": self.tenant_info['Name'], "table": "operator_working_time_summary_airflow"}
                new_last_run = {"last_run": self.end_date}
                utilfunction.update_dag_last_run(filter, new_last_run)
                return

            q = f"select * from flow_transaction_events where time>'{temp_data}' and time<='{self.end_date}' and (((butler_movement='arrival' or butler_movement='departure' or event_name = 'safety_pause_start' or event_name = 'safety_pause_end' ) and pps_mode='pick_mode') or event_name='wait_for_msu_start') order by time"
            df_main = pd.DataFrame(self.read_client.query(q).get_points())
            if not df_main.empty and 'carrier_type' in df_main.columns:
                df_main = df_main[df_main['carrier_type'] != 'packing_box']

            if not df_main.empty:
                if 'uom' not in df_main.columns:
                    df_main['uom'] = 'undefined'

                if 'pps_point' not in df_main.columns:
                    df_main['pps_point'] = 'undefined'

                df_main = df_main.sort_values(by=['installation_id', 'pps_id', 'time'], ascending=[True, True, True])
                df_main = utilfunction.reset_index(df_main)

                ttp_setup = (self.tenant_info['is_ttp_setup'] == 'Y')
                df_main = utilfunction.update_station_type(df_main, ttp_setup)
                df_main = utilfunction.update_storage_type(df_main, ttp_setup)

                df_main['prev_installation_id'] = df_main['installation_id'].shift(1)
                df_main['prev_pps_id'] = df_main['pps_id'].shift(1)
                df_main['prev_event_name'] = df_main['event_name'].shift(1)
                df_main['prev_rack_id'] = df_main['rack_id'].shift(1)
                df_main = df_main[((df_main['event_name'] != df_main['prev_event_name']) | (
                                           df_main['pps_id'] != df_main['prev_pps_id']) | (
                                           df_main['installation_id'] != df_main['prev_installation_id']) )]
                if not df_main.empty:
                    l = ['prev_installation_id', 'prev_pps_id', 'prev_event_name', 'prev_rack_id']
                    for col in l:
                        if col in df_main.columns:
                            del df_main[col]
                df_main['time'] = pd.to_datetime(df_main['time'], utc=True)
                if 'operator_id' not in df_main.columns:
                    df_main['operator_id'] = "default"
                df_main['breach_start_time'] = np.nan
                df_main['breach_end_time'] = np.nan                       
                df_main['transaction_start_time'] = np.nan
                df_main['transaction_end_time'] = np.nan
                df_main['pptl_event_processed_time'] = np.nan
                df_main['breach_start_time'] = df_main.apply(
                    lambda x: x['time'] if x['event_name'] == 'safety_pause_start' else np.nan, axis=1)
                df_main['breach_end_time'] = df_main.apply(
                    lambda x: x['time'] if x['event_name'] == 'safety_pause_end' else np.nan, axis=1)                   
                df_main['transaction_start_time'] = df_main.apply(
                    lambda x: x['time'] if x['butler_movement'] == 'arrival' else np.nan, axis=1)
                df_main['transaction_end_time'] = df_main.apply(
                    lambda x: x['time'] if x['butler_movement'] == 'departure' else np.nan, axis=1)
                # df_main['pptl_event_processed_time'] = df_main.apply(
                #     lambda x: x['time'] if (x['event_name'] == 'pptl_event_processed' and x[
                #         'event_type'] == 'product_placed_in_bin') else np.nan, axis=1)
                df_main['pps_mode'] = df_main['pps_mode'].apply(
                    lambda x: x.replace('_mode', '') if not pd.isna(x) and len(x) > 4 else x)
                # if 'butler_movement' in df_main.columns:
                #     df_main['arrival_time'] = df_main.apply(
                #         lambda x: x['time'] if x['butler_movement'] == 'arrival' else np.nan, axis=1)

                df_main['transaction_group'] = 0

                df_main = utilfunction.reset_index(df_main)
                df_main = self.apply_transaction_group2(df_main)
                # df_main.to_csv(r'/home/ankush.j/Desktop/workname/big_data/df_main1.csv', index=False, header=True)
                df_main = df_main.sort_values(
                    by=['installation_id', 'pps_id', 'time', 'rack_id'],
                    ascending=[True, True, True, True])
                df_main = df_main.groupby(
                    ['installation_id', 'pps_id', 'pps_mode', 'transaction_group'],
                    as_index=False).agg(
                    time=('time', 'min'),
                    rack_id=('rack_id', 'first'),
                    storage_type = ('storage_type', 'first'),
                    station_type = ('station_type', 'first'),
                    operator_id=('operator_id', 'first'),
                    first_transaction_start_time=('transaction_start_time', 'min'),
                    pptl_event_processed_time=('transaction_end_time', 'max'),
                   # next_rack_arrival_time=('arrival_time', 'max'),
                    time_end=('time', 'max'),
                    breach_start = ('breach_start_time','min'),
                    breach_end = ('breach_end_time','max'))

                df_main = df_main[(~pd.isna(df_main['rack_id']))]
                df_main = df_main[~(pd.isna(df_main['pptl_event_processed_time']))]
                df_main = df_main[~(pd.isna(df_main['first_transaction_start_time']))]
                df_main['breach_start'] = df_main.apply(lambda x : max(x['breach_start'], x['pptl_event_processed_time']) if pd.notna(x['pptl_event_processed_time']) and pd.notna(x['breach_start']) else x['breach_start'], axis = 1)
                if df_main.empty:
                    return

                df_main = df_main.sort_values(
                    by=['installation_id', 'pps_id', 'pps_mode', 'transaction_group', 'time', 'rack_id'],
                    ascending=[True, True, True, True, True, True])
                df_main = utilfunction.reset_index(df_main)
                df_main['first_transaction_start_time_upper'] = df_main['first_transaction_start_time'].shift(-1)
                df_main['pps_id_upper'] = df_main['pps_id'].shift(-1)
                df_main['installation_id_upper'] = df_main['installation_id'].shift(-1)
                df_main['first_transaction_start_time_upper'] = df_main.apply(lambda x:x['first_transaction_start_time_upper'] if x['installation_id']==x['installation_id_upper'] and x['pps_id']==x['pps_id_upper'] else np.nan, axis=1)
                df_main['next_rack_arrival_time'] = df_main['first_transaction_start_time_upper']
                df_main['opertor_working_time'] = np.nan
                df_main['breach_time'] = np.nan  
                df_main = utilfunction.datediff_in_millisec(df_main, 'breach_end',
                                                                'breach_start',
                                                                'breach_time')
                                                                                            
                df_main = utilfunction.datediff_in_millisec(df_main, 'pptl_event_processed_time',
                                                            'first_transaction_start_time',
                                                            'opertor_working_time')
                df_main['operator_waiting_time'] = np.nan

                df_main = utilfunction.datediff_in_millisec(df_main, 'first_transaction_start_time_upper',
                                                            'pptl_event_processed_time',
                                                            'operator_waiting_time')

                df_main = utilfunction.datediff_in_millisec(df_main, 'time_end',
                                                                'pptl_event_processed_time',
                                                                'operator_waiting_time')

                df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = df_main['operator_waiting_time']
                # df_main = utilfunction.datediff_in_millisec(df_main, 'next_rack_arrival_time',
                #                                             'pptl_event_processed_time',
                #                                             'time_diff_b_w_pptl_press_to_rack_arrival')
                df_main['next_rack_arrival_time']=df_main['first_transaction_start_time_upper']
                df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = df_main['time_diff_b_w_pptl_press_to_rack_arrival'].apply(lambda x: 0 if x <= 0 or pd.isna(x) else x)
                df_main['breach_time'] = df_main['breach_time'].fillna(0)
                df_main["ntile"] = 0
                df_main=self.apply_ntile(df_main)
                df_main = df_main.groupby(
                    ['installation_id', 'pps_id', 'pps_mode', 'ntile'],
                    as_index=False).agg(
                    time=('time', 'min'),
                    rack_id=('rack_id', 'first'),
                    operator_id=('operator_id', 'first'),
                    storage_type = ('storage_type', 'first'),
                    station_type = ('station_type', 'first'),
                    first_transaction_start_time=('first_transaction_start_time','min'),
                    pptl_event_processed_time=('pptl_event_processed_time', 'max'),
                    next_rack_arrival_time=('next_rack_arrival_time', 'max'),
                    operator_waiting_time=('operator_waiting_time', 'sum'),
                    opertor_working_time=('opertor_working_time', 'sum'),
                    time_diff_b_w_pptl_press_to_rack_arrival=('time_diff_b_w_pptl_press_to_rack_arrival', 'sum'),
                    breach_time = ('breach_time','sum'),)

                df_main['operator_waiting_time'] = df_main['operator_waiting_time'].astype(int, errors='ignore')
                df_main['opertor_working_time'] = df_main['opertor_working_time'].astype(int, errors='ignore')
                df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = df_main['time_diff_b_w_pptl_press_to_rack_arrival'].astype(int, errors='ignore')
                df_main['pps_id'] = df_main['pps_id'].astype(str, errors='ignore')
                df_main['rack_id'] = df_main['rack_id'].astype(str, errors='ignore')
                df_main['operator_id'] = df_main['operator_id'].astype(str, errors='ignore')

                df_main["start"] = pd.to_datetime(self.start_date) - timedelta(minutes=30)
                df_main["flag2"] = df_main.apply(self.date_compare, axis=1)
                df_main = df_main[(df_main["flag2"])]
                if not df_main.empty:
                    df_main = df_main.drop(['flag2', 'start'], axis=1)

                if not df_main.empty :
                    l = ['first_transaction_start_time_upper', 'pps_id_upper', 'installation_id_upper', 'prev_rack_id','transaction_group','time_end','index','ntile','start']
                    for col in l:
                        if col in df_main.columns:
                            del df_main[col]
                    df_main[['opertor_working_time', 'operator_waiting_time', 'time_diff_b_w_pptl_press_to_rack_arrival']] = df_main[['opertor_working_time', 'operator_waiting_time', 'time_diff_b_w_pptl_press_to_rack_arrival']].fillna(0)
                    df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = df_main.apply(lambda x : x['time_diff_b_w_pptl_press_to_rack_arrival'] - x['breach_time'] if (x['time_diff_b_w_pptl_press_to_rack_arrival'] - x['breach_time'])>0 else 0, axis = 1)
                    df_main['time_diff_b_w_pptl_press_to_rack_arrival'] = df_main['time_diff_b_w_pptl_press_to_rack_arrival'].astype(int)
                    df_main['breach_time'] = df_main['breach_time'].astype(int)

                    df_main['Actual_operator_working_time']=df_main['opertor_working_time']
                    df_main['Actual_operator_working_time'] = df_main['Actual_operator_working_time'].apply(lambda x: 0 if x <= 0 or pd.isna(x) else x)
                    df_main.first_transaction_start_time = df_main.first_transaction_start_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(x) else x)
                    df_main.pptl_event_processed_time = df_main.pptl_event_processed_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(x) else x)
                    df_main.next_rack_arrival_time = df_main.next_rack_arrival_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(x) else x)
                    df_main['first_transaction_start_time'] = df_main['first_transaction_start_time'].astype(str, errors='ignore')
                    df_main['pptl_event_processed_time'] = df_main['pptl_event_processed_time'].astype(str, errors='ignore')
                    df_main['next_rack_arrival_time'] = df_main['next_rack_arrival_time'].astype(str, errors='ignore')


                    df_main.time = pd.to_datetime(df_main.time)
                    df_main = df_main.set_index('time')
                    self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                         port=self.tenant_info["write_influx_port"])
                    self.write_client.writepoints(df_main, "operator_working_time_summary_airflow",
                                                  db_name=self.tenant_info["alteryx_out_db_name"],
                                                  tag_columns=['pps_id'],
                                                  dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                else:
                    filter = {"site": self.tenant_info['Name'], "table": "operator_working_time_summary_airflow"}
                    new_last_run = {"last_run": self.end_date}
                    self.utilfunction.update_dag_last_run(filter, new_last_run)

        return None

