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
class Operator_working_time:

    def remove_unnecassary_tag_keys(self):
        q = f"SHOW TAG KEYS FROM operator_working_time_airflow"
        df = pd.DataFrame(self.client.query(q).get_points())
        if not df.empty and 'rack_id' in df['tagKey'].values:
            self.client.query("drop measurement operator_working_time_airflow")
            self.client.query("delete from last_dag_run_status where table = 'operator_working_time_airflow'")

    def apply_transaction_group(self, df):
        group = 0
        for x in df.index - 1:
            if x >= 0:
                if ((df['event_name'][x] == 'pptl_event_processed' and df['event_type'][x] == 'product_placed_in_bin') or
                        (df['pps_id'][x] != df['pps_id'][x + 1]) or
                        (df['installation_id'][x] != df['installation_id'][x + 1]) or
                        (df['event_name'][x] == 'transaction_end') or
                        (df['event_name'][x] == 'cancel_scan')
                ):
                    group = group + 1
                df['transaction_group'][x + 1] = group
        return df

    def update_breach_time(self, df):
        for x in df.index - 1:
            if x >= 0:
                if df['installation_id'][x] == df['installation_id'][x + 1] and df['pps_id'][x] == df['pps_id'][
                    x + 1] and df['rack_id'][x] == df['rack_id'][x + 1]:
                    df['breach_time'][x] = df['breach_time'][x + 1]
        return df

    def apply_clean_exception_start(self, df):
        group = 0
        for x in df.index - 1:
            if x >= 0:
                if df['event_name'][x] == 'exception_cancel':
                    i = 1
                    while (i <= 10 and x - i >= 0):
                        if df['event_name'][x - i] == 'exception_start':
                            df['exception_start_time'][x - i] = np.nan
                            break
                        i = i + 1
        return df

    def apply_departure_time(self, df):
        final_df= df[((df['butler_movement']=='arrival')|(df['butler_movement'] == 'departure'))]
        final_df= final_df.sort_values(by=['installation_id', 'pps_id', 'pps_point', 'time'],  ascending=[True, True,  True,True])
        final_df['departure_time_lower'] = final_df['departure_time'].shift(+1)
        final_df['pps_point_lower'] = final_df['pps_point'].shift(+1)
        if not final_df.empty:
            final_df['departure_time'] = final_df.apply(lambda x:x['departure_time_lower'] if x['butler_movement']=='arrival' and x['pps_point']==x['pps_point_lower']else x['departure_time'],axis=1)
            del df['departure_time']
            df =pd.merge(df,final_df[['installation_id','pps_id','pps_mode','rack_id','pps_point','arrival_time','departure_time']],on =['installation_id','pps_id','pps_mode','rack_id','pps_point','arrival_time'],how ='left')
        # for x in df.index - 1:
        #     if x > 0:
        #         if pd.isna(df['departure_time'][x]) and df['pps_id'][x] == df['pps_id'][x - 1]:
        #             df['departure_time'][x] = df['departure_time'][x - 1]
        #
        #         if pd.isna(df['arrival_time'][x]) and df['pps_id'][x] == df['pps_id'][x - 1]:
        #             df['arrival_time'][x] = df['arrival_time'][x - 1]
        return df

    def apply_clear_rack_arrival_time(self, df):
        for x in df.index - 1:
            if x > 0:
                if df['waiting_for_rack_to_arrive'][x] == df['waiting_for_rack_to_arrive'][x - 1] and \
                        df['waiting_for_rack_to_arrive'][x] > 0:
                    df['waiting_for_rack_to_arrive'][x - 1] = 0

        return df

    def Operator_working_time(self, tenant_info, **kwargs):
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

        check_start_date = self.client.get_start_date( "operator_working_time_airflow", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        #self.remove_unnecassary_tag_keys()

        q = f"select *  from flow_transaction_events where time>'{check_start_date}' and time<='{check_end_date}' and (event_name='pptl_event_processed') and pps_mode ='pick' limit 1"
        df_flow_transaction_events = pd.DataFrame(self.read_client.query(q).get_points())
        if df_flow_transaction_events.empty:
            self.end_date = datetime.now(timezone.utc)
            self.Operator_working_time1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("operator_working_time_airflow", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.Operator_working_time1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def Operator_working_time1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("operator_working_time_airflow", self.tenant_info)
        self.original_start_date = pd.to_datetime(self.start_date) + timedelta(minutes=10)
        self.start_date = pd.to_datetime(self.start_date) - timedelta(minutes=30)
        print(f"start date: {self.start_date}, end_date : {self.end_date}")
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        utilfunction = CommonFunction()

        q = f"select * from flow_transaction_events where time>'{self.original_start_date}' and time<='{self.end_date}'  and event_name='pptl_event_processed' and pps_mode ='pick' limit 1"
        df_main = pd.DataFrame(self.read_client.query(q).get_points())
        if df_main.empty:
            filter = {"site": self.tenant_info['Name'], "table": "operator_working_time_airflow"}
            new_last_run = {"last_run": self.end_date}
            utilfunction.update_dag_last_run(filter,new_last_run)
            return

        q = f"select * from flow_transaction_events where time>'{self.start_date}' and time<='{self.end_date}' and (pps_mode='pick' or pps_mode='pick_mode') and event_name!='login' and event_name !='logout'   order by time"
        df_main = pd.DataFrame(self.read_client.query(q).get_points())
        if df_main.empty:
            return
        if not df_main.empty:
            if 'carrier_type' in df_main.columns:
                df_main = df_main[df_main['carrier_type'] != 'packing_box']

            if 'uom' not in df_main.columns:
                df_main['uom'] = 'undefined'

            if 'pps_point' not in df_main.columns:
                df_main['pps_point'] = 'undefined'

            df_main = df_main.sort_values(by=['installation_id', 'pps_id', 'time'], ascending=[True, True, True])
            df_main = utilfunction.reset_index(df_main)

            ttp_setup = (self.tenant_info['is_ttp_setup']=='Y')
            df_main = utilfunction.update_station_type(df_main,ttp_setup)
            df_main = utilfunction.update_storage_type(df_main,ttp_setup)

            df_main['prev_installation_id'] = df_main['installation_id'].shift(1)
            df_main['prev_pps_id'] = df_main['pps_id'].shift(1)
            df_main['prev_event_name'] = df_main['event_name'].shift(1)
            df_main['prev_butler_movement'] = df_main['butler_movement'].shift(1)
            df_main['prev_rack_id'] = df_main['rack_id'].shift(1)
            df_main = df_main[((df_main['event_name'] != df_main['prev_event_name']) & (
                    df_main['butler_movement'] != df_main['prev_butler_movement']) | (
                                       df_main['pps_id'] != df_main['prev_pps_id']) | (
                                       df_main['installation_id'] != df_main['prev_installation_id']) | (
                                       df_main['rack_id'] != df_main['prev_rack_id']))]
            if not df_main.empty:
                l = ['prev_installation_id', 'prev_pps_id', 'prev_event_name', 'prev_butler_movement', 'prev_rack_id']
                for col in l:
                    if col in df_main.columns:
                        del df_main[col]
            df_main['time'] = pd.to_datetime(df_main['time'], utc=True)
            if 'operator_id' not in df_main.columns:
                df_main['operator_id'] = "default"
            df_main['transaction_start_time'] = np.nan
            df_main['transaction_end_time'] = np.nan
            df_main['product_scan_time'] = np.nan
            df_main['scan_complete_time'] = np.nan
            df_main['pptl_event_processed_time'] = np.nan
            df_main['wait_for_msu_start_time'] = np.nan
            df_main['exception_start_time'] = np.nan
            df_main['exception_end_time'] = np.nan
            df_main['arrival_time'] = np.nan
            df_main['departure_time'] = np.nan
            df_main['breach_time'] = np.nan
            df_main['product_placed_in_bin_time'] = np.nan
            df_main['carrier_associated_time'] = np.nan
            df_main['system_idle_start_time'] = np.nan
            df_main['carrier_scan_time'] = np.nan
            df_main['waiting_for_free_bin_start_time'] = np.nan
            df_main['cancel_scan_time'] = np.nan
            df_main['transaction_start_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'transaction_start' else np.nan, axis=1)
            df_main['transaction_end_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'transaction_end' else np.nan, axis=1)
            df_main['product_scan_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'product_scan' or x[
                    'event_name'] == 'scan_events' else np.nan, axis=1)
            df_main['scan_complete_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'scan_complete' or x[
                    'event_name'] == 'slot_scan_complete' else np.nan, axis=1)
            df_main['pptl_event_processed_time'] = df_main.apply(
                lambda x: x['time'] if (x['event_name'] == 'pptl_event_processed' and x[
                    'event_type'] == 'product_placed_in_bin') else np.nan, axis=1)
            df_main['carrier_associated_time'] = df_main.apply(
                lambda x: x['time'] if (x['event_name'] == 'pptl_event_processed' and x[
                    'event_type'] == 'carrier_associated') else np.nan, axis=1)
            df_main['wait_for_msu_start_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'wait_for_msu_start' else np.nan, axis=1)
            df_main['carrier_scan_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'carrier_scan' else np.nan, axis=1)
            df_main['system_idle_start_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'system_idle_start' else np.nan, axis=1)
            df_main['waiting_for_free_bin_start_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'waiting_for_free_bin_start' else np.nan, axis=1)
            df_main['exception_start_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'exception_start' else np.nan, axis=1)
            df_main['exception_end_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'exception_end' else np.nan, axis=1)
            df_main['cancel_scan_time'] = df_main.apply(
                lambda x: x['time'] if x['event_name'] == 'cancel_scan' else np.nan, axis=1)
            if 'butler_movement' in df_main.columns:
                df_main['arrival_time'] = df_main.apply(
                    lambda x: x['time'] if x['butler_movement'] == 'arrival' else np.nan, axis=1)
                df_main['departure_time'] = df_main.apply(
                    lambda x: x['time'] if x['butler_movement'] == 'departure' else np.nan, axis=1)

            if 'breach_status' in df_main.columns:
                df_main['breach_time'] = df_main.apply(
                    lambda x: x['time'] if x['breach_status'] == 'breach' else np.nan, axis=1)
            # if 'event_type' in df_main.columns:
            #     df_main['product_placed_in_bin_time'] = df_main.apply(
            #         lambda x: x['time'] if x['event_type'] == 'product_placed_in_bin' else np.nan, axis=1)
            #     df_main['carrier_associated_time'] = df_main.apply(
            #         lambda x: x['time'] if x['event_type'] == 'carrier_associated' else np.nan, axis=1)
            df_main['pps_mode'] = df_main['pps_mode'].apply(
                lambda x: x.replace('_mode', '') if not pd.isna(x) and len(x) > 4 else x)
            df_main['transaction_group'] = 0

            df_main = utilfunction.reset_index(df_main)
            df_main = self.apply_transaction_group(df_main)
            # df_main = self.apply_butler_movement_group(df_main)
            df_main = self.apply_clean_exception_start(df_main)
            df_main = self.apply_departure_time(df_main)
            df_main['waiting_for_rack_to_arrive'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'arrival_time', 'departure_time',
                                                        'waiting_for_rack_to_arrive')
            df_main['waiting_for_rack_to_arrive'] = df_main['waiting_for_rack_to_arrive'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['waiting_for_rack_to_arrive'] = df_main.apply(
                lambda x: 0 if x['event_name'] == 'wait_for_msu_start' else x['waiting_for_rack_to_arrive'], axis=1)
            # df_main['arrival_time'] = df_main.apply(lambda x: x['arrival_time'] if x['butler_movement']=='arrival' else np.nan , axis=1)
            df_main['rack_id'] = df_main['rack_id'].apply(lambda x: np.nan if x == 'undefined' else x)
            df_main['rack_id'] = df_main['rack_id'].apply(lambda x: int(x) if not pd.isna(x) and x.isnumeric() else x)
            df_main['temp_rack_id'] = df_main.apply(
                lambda x: x['rack_id'] if (x['event_name'] == 'transaction_start' or x['event_name'] == 'rack_inducted') else np.nan, axis=1)
            # df_main.to_csv(r'/home/ankush.j/Desktop/workname/big_data/df_main1.csv', index=False, header=True)
            df_main = df_main.sort_values(
                by=['installation_id', 'pps_id', 'time', 'rack_id'],
                ascending=[True, True, True, True])
            df_main = self.apply_clear_rack_arrival_time(df_main)
            df_main = df_main.groupby(
                ['installation_id', 'pps_id', 'pps_mode', 'transaction_group'],
                as_index=False).agg(
                time=('time', 'min'),
                rack_id=('temp_rack_id', 'first'),
                uom_type=('uom', 'last'),
                operator_id=('operator_id', 'first'),
                station_type=('station_type','first'),
                storage_type=('storage_type','first'),
                last_transaction_start_time=('transaction_start_time', 'max'),
                first_transaction_start_time=('transaction_start_time', 'min'),
                audit_transaction_end_time=('transaction_end_time', 'max'),
                min_product_scan_time=('product_scan_time', 'min'),
                product_scan_time=('product_scan_time', 'max'),
                scan_complete_time=('scan_complete_time', 'max'),
                pptl_event_processed_time=('pptl_event_processed_time', 'max'),
                carrier_associated_time=('carrier_associated_time','max'),
                wait_for_msu_start_time=('wait_for_msu_start_time', 'min'),
                wait_for_msu_end_time=('wait_for_msu_start_time', 'max'),
                exception_start_time=('exception_start_time', 'min'),
                exception_end_time=('exception_end_time', 'max'),
                arrival_time=('arrival_time', 'min'),
                # departure_time=('departure_time', 'min'),
                waiting_for_rack_to_arrive=('waiting_for_rack_to_arrive', 'max'),
                breach_time=('breach_time', 'min'),
                # product_placed_in_bin_time=('product_placed_in_bin_time', 'min'),
                # carrier_associated_time=('carrier_associated_time', 'min'),
                system_idle_start_time=('system_idle_start_time', 'min'),
                system_idle_end_time=('system_idle_start_time', 'max'),
                carrier_scan_time=('carrier_scan_time', 'min'),
                waiting_for_free_bin_start_time=('waiting_for_free_bin_start_time', 'min'),
                cancel_scan_time=('cancel_scan_time', 'max'),
                slot_id = ('slot_id','last'),
                sku_id = ('sku_id','last')
                )
            df_main = df_main[(~pd.isna(df_main['rack_id']))]
            df_main =df_main[~(pd.isna(df_main['pptl_event_processed_time']) & pd.isna(df_main['cancel_scan_time']))]
            if df_main.empty:
                return
            df_main = df_main.sort_values(
                by=['installation_id', 'pps_id', 'pps_mode', 'transaction_group', 'rack_id', 'time'],
                ascending=[True, True, True, True, True, True])
            df_main = utilfunction.reset_index(df_main)
            df_main['rack_id_lower'] = df_main['rack_id'].shift(+1)
            df_main['pptl_event_processed_time_lower'] = df_main['pptl_event_processed_time'].shift(+1)
            df_main['first_transaction_start_time_upper'] = df_main['first_transaction_start_time'].shift(-1)
            df_main['product_scan_time_upper'] = df_main['product_scan_time'].shift(-1)
            df_main['first_transaction_start_time_upper'] = df_main.apply(
                lambda x: x['product_scan_time_upper'] if pd.isna(x['first_transaction_start_time_upper']) else x[
                    'first_transaction_start_time_upper'], axis=1)
            df_main['cancel_scan_time_lower'] = df_main['cancel_scan_time'].shift(+1)
            df_main['pptl_event_processed_time_lower'] = df_main.apply(lambda x:x['cancel_scan_time_lower'] if pd.isna(x['pptl_event_processed_time_lower']) else x['pptl_event_processed_time_lower'], axis=1 )
            df_main['tote_to_tote_transaction_time'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'first_transaction_start_time',
                                                        'pptl_event_processed_time_lower',
                                                        'tote_to_tote_transaction_time')
            df_main['tote_to_tote_transaction_time'] = df_main.apply(
                lambda x: x['tote_to_tote_transaction_time'] if x['rack_id_lower'] != x['rack_id'] else 0, axis=1)

            # df_main.to_csv(r'/home/ankush.j/Desktop/workname/big_data/df_main.csv', index=False, header=True)
            df_main = self.update_breach_time(df_main)
            df_main['rack_waiting_time'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'first_transaction_start_time', 'arrival_time',
                                                        'rack_waiting_time')

            df_main['first_product_scan'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'min_product_scan_time',
                                                        'first_transaction_start_time', 'first_product_scan')

            df_main['time_to_pick_item'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'first_transaction_start_time_upper', 'product_scan_time',
                                                        'time_to_pick_item')
            df_main['time_to_scan_items'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'scan_complete_time', 'product_scan_time',
                                                        'time_to_scan_items')
            df_main['time_to_put_items_into_bin'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'pptl_event_processed_time', 'scan_complete_time',
                                                        'time_to_put_items_into_bin')
            df_main['time_to_walk_back'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'pptl_event_processed_time', 'breach_time',
                                                        'time_to_walk_back')
            df_main['time_to_scan_and_associate_tote_or_pb_to_bin'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'carrier_associated_time', 'carrier_scan_time',
                                                        'time_to_scan_and_associate_tote_or_pb_to_bin')

            df_main['waiting_for_tote_pb_association'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'carrier_scan_time', 'wait_for_msu_start_time',
                                                        'waiting_for_tote_pb_association')
            df_main['waiting_for_bin_to_get_cleared'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'first_transaction_start_time',
                                                        'waiting_for_free_bin_start_time',
                                                        'waiting_for_bin_to_get_cleared')
            df_main['system_idle'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'system_idle_end_time', 'system_idle_start_time',
                                                        'system_idle')
            df_main['tote_Waiting_time'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'first_transaction_start_time',
                                                        'wait_for_msu_start_time',
                                                        'tote_Waiting_time')
            df_main['time_spent_on_raising_item_exceptions'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'exception_end_time', 'exception_start_time',
                                                        'time_spent_on_raising_item_exceptions')
            df_main = utilfunction.datediff_in_millisec(df_main, 'cancel_scan_time', 'exception_start_time',
                                                        'time_spent_on_raising_item_exceptions')

            df_main['bin_full'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'exception_start_time', 'first_transaction_start_time',
                                                        'bin_full')

            df_main['opertor_working_time'] = np.nan
            df_main['operator_waiting_time'] = np.nan
            df_main['product_scan_to_pptl_press'] = np.nan

            df_main = utilfunction.datediff_in_millisec(df_main, 'wait_for_msu_end_time',
                                                        'wait_for_msu_start_time',
                                                        'operator_waiting_time')
            
            df_main = utilfunction.datediff_in_millisec(df_main, 'pptl_event_processed_time',
                                                        'first_transaction_start_time',
                                                        'opertor_working_time')

            df_main = utilfunction.datediff_in_millisec(df_main, 'audit_transaction_end_time',
                                                        'first_transaction_start_time',
                                                        'opertor_working_time')
            
            df_main = utilfunction.datediff_in_millisec(df_main, 'pptl_event_processed_time',
                                                        'min_product_scan_time',
                                                        'product_scan_to_pptl_press')

            df_main['bin_skip'] = np.nan
            df_main = df_main.drop(
                columns=['transaction_group', 'index', 'last_transaction_start_time', 'first_transaction_start_time',
                         'product_scan_time',
                         'scan_complete_time', 'pptl_event_processed_time', 'wait_for_msu_start_time','wait_for_msu_end_time',
                         'exception_start_time','system_idle_end_time',
                         'exception_end_time', 'breach_time', 'carrier_associated_time',
                         'system_idle_start_time', 'waiting_for_free_bin_start_time', 'carrier_scan_time',
                         'arrival_time', 'rack_id_lower', 'pptl_event_processed_time_lower',
                         'audit_transaction_end_time', 'min_product_scan_time','cancel_scan_time','cancel_scan_time_lower','first_transaction_start_time_upper','product_scan_time_upper'
                         ])

            self.original_start_date = pd.to_datetime(self.original_start_date, utc=True) - timedelta(minutes=10)
            temp_df_main = df_main[(df_main['time'] < self.original_start_date)]
            temp_df_main = temp_df_main.sort_values(by=['installation_id', 'pps_id', 'time'],
                                                    ascending=[True, True, True])
            temp_df_main = temp_df_main.drop_duplicates(subset=['installation_id', 'pps_id'], keep='last')
            df_main = df_main[(df_main['time'] >= self.original_start_date)]
            df_main = pd.concat([temp_df_main, df_main])
            df_main = utilfunction.reset_index(df_main)
            df_main = df_main.set_index('time')
            df_main['bin_full'] = df_main['bin_full'].apply(lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['first_product_scan'] = df_main['first_product_scan'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['rack_waiting_time'] = df_main['rack_waiting_time'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['system_idle'] = df_main['system_idle'].apply(lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['waiting_for_bin_to_get_cleared'] = df_main['waiting_for_bin_to_get_cleared'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['time_spent_on_raising_item_exceptions'] = df_main['time_spent_on_raising_item_exceptions'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['time_to_pick_item'] = df_main['time_to_pick_item'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['time_to_put_items_into_bin'] = df_main['time_to_put_items_into_bin'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['time_to_scan_and_associate_tote_or_pb_to_bin'] = df_main[
                'time_to_scan_and_associate_tote_or_pb_to_bin'].apply(lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['time_to_scan_items'] = df_main['time_to_scan_items'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['waiting_for_rack_to_arrive'] = df_main['waiting_for_rack_to_arrive'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['waiting_for_tote_pb_association'] = df_main['waiting_for_tote_pb_association'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['tote_Waiting_time'] = df_main['tote_Waiting_time'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['operator_waiting_time'] = df_main['operator_waiting_time'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)  
            df_main['product_scan_to_pptl_press'] = df_main['product_scan_to_pptl_press'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)       

            df_main['product_scan_to_pptl_press'] = df_main['product_scan_to_pptl_press'].astype(float, errors='ignore')
            df_main['time_to_pick_item'] = df_main['time_to_pick_item'].astype(float, errors='ignore')
            df_main['first_product_scan'] = df_main['first_product_scan'].astype(float, errors='ignore')
            df_main['time_to_scan_items'] = df_main['time_to_scan_items'].astype(float, errors='ignore')
            df_main['time_to_put_items_into_bin'] = df_main['time_to_put_items_into_bin'].astype(float, errors='ignore')
            df_main['time_to_walk_back'] = df_main['time_to_walk_back'].astype(float, errors='ignore')
            df_main['time_to_scan_and_associate_tote_or_pb_to_bin'] = df_main[
                'time_to_scan_and_associate_tote_or_pb_to_bin'].astype(float, errors='ignore')
            df_main['waiting_for_rack_to_arrive'] = df_main['waiting_for_rack_to_arrive'].astype(float, errors='ignore')
            df_main['waiting_for_tote_pb_association'] = df_main['waiting_for_tote_pb_association'].astype(float,
                                                                                                           errors='ignore')
            df_main['waiting_for_bin_to_get_cleared'] = df_main['waiting_for_bin_to_get_cleared'].astype(float,
                                                                                                         errors='ignore')
            df_main['system_idle'] = df_main['system_idle'].astype(float, errors='ignore')
            df_main['time_spent_on_raising_item_exceptions'] = df_main['time_spent_on_raising_item_exceptions'].astype(
                float, errors='ignore')
            df_main['bin_full'] = df_main['bin_full'].astype(float, errors='ignore')
            df_main['bin_skip'] = df_main['bin_skip'].astype(float, errors='ignore')
            df_main['tote_Waiting_time'] = df_main['tote_Waiting_time'].astype(float, errors='ignore')
            df_main['rack_id2'] = df_main['rack_id']  # without tag key
            df_main['opertor_working_time'] = df_main['opertor_working_time'].astype(float, errors='ignore')
            df_main['waiting_for_rack_to_arrive'] = df_main['waiting_for_rack_to_arrive'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['waiting_for_rack_to_arrive'] = df_main['waiting_for_rack_to_arrive'].astype(float, errors='ignore')
            df_main['tote_to_tote_transaction_time2'] = df_main['tote_to_tote_transaction_time']
            df_main['tote_to_tote_transaction_time2'] = df_main['tote_to_tote_transaction_time2'].apply(
                lambda x: 0 if x <= 0 or pd.isna(x) else x)
            df_main['tote_to_tote_transaction_time2'] = df_main['tote_to_tote_transaction_time2'].astype(float,
                                                                                                         errors='ignore')
            df_main['tote_to_tote_transaction_time'] = df_main['tote_to_tote_transaction_time'].astype(str,
                                                                                                       errors='ignore')
            q = "SHOW FIELD KEYS FROM  operator_working_time_airflow"
            df = pd.DataFrame(self.client.query(q).get_points())
            rack_waiting_time_type = "float"
            rack_d2_type = "string"
            rack_id_type = "string"
            if not df.empty:
                rack_waiting_time_type = df[df['fieldKey'] == 'rack_waiting_time']
                rack_d2_type = df[df['fieldKey'] == 'rack_id2']
                rack_id_type = df[df['fieldKey'] == 'rack_id']
                if not rack_waiting_time_type.empty:
                    rack_waiting_time_type = rack_waiting_time_type.reset_index()
                    rack_waiting_time_type = rack_waiting_time_type['fieldType'][0]
                else:
                    rack_waiting_time_type = "float"

                if not rack_d2_type.empty:
                    rack_d2_type = rack_d2_type.reset_index()
                    rack_d2_type = rack_d2_type['fieldType'][0]
                else:
                    rack_d2_type = "string"

                if not rack_id_type.empty:
                    rack_id_type = rack_id_type.reset_index()
                    rack_id_type = rack_id_type['fieldType'][0]
                else:
                    rack_id_type = "string"

            if 'rack_waiting_time' in df_main.columns:
                if rack_waiting_time_type == 'float':
                    df_main['rack_waiting_time'] = df_main['rack_waiting_time'].astype(float)
                elif rack_waiting_time_type == 'integer':
                    df_main['rack_waiting_time'] = df_main['rack_waiting_time'].astype(int)
                else:
                    df_main = df_main.drop(columns=['rack_waiting_time'])

            if 'rack_id2' in df_main.columns:
                if rack_d2_type == 'float':
                    df_main['rack_id2'] = df_main['rack_id2'].astype(float)
                elif rack_d2_type == 'string':
                    df_main['rack_id2'] = df_main['rack_id2'].astype(str)
                else:
                    df_main = df_main.drop(columns=['rack_id2'])
            else:
                df_main['rack_id2'] = df_main['rack_id2'].astype(str)

            if 'rack_id' in df_main.columns:
                if rack_id_type == 'float':
                    df_main['rack_id'] = df_main['rack_id'].astype(float)
                else:
                    df_main['rack_id'] = df_main['rack_id'].astype(str)
            else:
                df_main['rack_id'] = df_main['rack_id'].astype(str)

            df_main['face'] = df_main['slot_id'].apply(lambda x: x.split('.')[1] if ~(pd.isna(x)) and x!='undefined' else 'undefined')
            del df_main['slot_id']
            df_main['face'] = df_main['face'].astype(str)
            df_main['sku_id'] = df_main['sku_id'].astype(str)

            df_main['uom_type'] = df_main['uom_type'].apply(lambda x: 'undefined' if x == None or pd.isna(x) else x)
            df_main['uom_type'] = df_main['uom_type'].astype(str, errors='ignore')
            df_main['operator_waiting_time'] = df_main['operator_waiting_time'].astype(int, errors='ignore')
            df_main['bin_full'] = df_main['bin_full'].apply(lambda x:0.0 if x<=0 or pd.isna(x) else x)
            df_main['system_idle'] = df_main['system_idle'].apply(lambda x: 0.0 if x <= 0 or pd.isna(x) else x)
            df_main['waiting_for_bin_to_get_cleared'] = df_main['waiting_for_bin_to_get_cleared'].astype(float, errors='ignore')

            if df_main.empty:
                filter = {"site": self.tenant_info['Name'], "table": "operator_working_time_airflow"}
                new_last_run = {"last_run": self.end_date}
                utilfunction.update_dag_last_run(filter, new_last_run)
                return

            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_main, "operator_working_time_airflow",
                                          db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['pps_id'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            
        return None
    

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
        'Operator_working_time',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of operator_working_time',
        schedule_interval='55 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['operator_working_time'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='Operator_working_time_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(Operator_working_time().Operator_working_time,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='Operator_working_time_final',
            provide_context=True,
            python_callable=functools.partial(Operator_working_time().Operator_working_time,
                                              tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )