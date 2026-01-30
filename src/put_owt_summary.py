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
class put_operator_working_time_summary:

    def date_compare(self, x):
        diff = (x['start'] - x['time']).total_seconds()
        # diff = (x['start'].tz_localize(tz=pytz.UTC)-x['interval_start'].tz_localize(tz=pytz.UTC)).total_seconds()
        if diff <= 0:
            return True
        return False

    def apply_transaction_group(self, df):
        group = 0
        for x in df.index:
            if x > 0:
                if ((df['rack_id'][x] != df['rack_id'][x - 1]) or
                        (df['pps_id'][x] != df['pps_id'][x - 1]) or
                        (df['installation_id'][x] != df['installation_id'][x - 1])):
                    group = group + 1
                df['transaction_group'][x] = group
        return df


    def put_operator_working_time_summary(self, tenant_info, **kwargs):
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

        check_start_date = self.client.get_start_date("put_owt_summary", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        # self.remove_unnecassary_tag_keys()

        q = f"select * from flow_transaction_events where time>'{check_start_date}' and time<='{check_end_date}'  and ((event_name='slot_scan_complete' and flow_name='sdp') or event_name='send_msu') and (pps_mode='put' or pps_mode='put_mode') limit 1"
        df_flow_transaction_events = pd.DataFrame(self.read_client.query(q).get_points())
        if df_flow_transaction_events.empty:
            self.end_date = datetime.now(timezone.utc)
            self.put_operator_working_time_summary1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("put_owt_summary", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("put_owt_summary", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.put_operator_working_time_summary1(self.end_date, **kwargs)
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

    def put_operator_working_time_summary1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("put_owt_summary", self.tenant_info)
        temp_data= pd.to_datetime(self.start_date) - timedelta(minutes=60)
        self.start_date = pd.to_datetime(self.start_date)
        print(f"temp_data: {temp_data}, start date: {self.start_date}, end_date : {self.end_date}")
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        temp_data = temp_data.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        utilfunction = CommonFunction()

        q = f"select * from flow_transaction_events where time>'{self.start_date}' and time<='{self.end_date}'  and ((event_name='slot_scan_complete' and flow_name='sdp') or event_name='send_msu') and (pps_mode='put' or pps_mode='put_mode') limit 1"
        df_main = pd.DataFrame(self.read_client.query(q).get_points())
        if df_main.empty:
            filter = {"site": self.tenant_info['Name'], "table": "put_owt_summary"}
            new_last_run = {"last_run": self.end_date}
            utilfunction.update_dag_last_run(filter, new_last_run)
            return
        q = f"select * from flow_transaction_events where time>'{temp_data}' and time<='{self.end_date}' and (pps_mode='put' or pps_mode='put_mode') and ( \
            (event_name = 'transaction_start' or event_name = 'send_msu') or \
            (flow_name='sdp' and  event_name = 'slot_scan_complete') or  \
            (flow_name = 'udp_ind_exp' and event_name = 'rack_inducted') or \
            (flow_name = 'udp_ind_inv' and event_name = 'carrier_scan') ) order by time" 
                
        df_main = pd.DataFrame(self.read_client.query(q).get_points())
        df_main = df_main.fillna('')
        if df_main.empty:
            return
        if not df_main.empty:
            if 'carrier_type' in df_main.columns:
                df_main = df_main[df_main['carrier_type'] != 'packing_box']

            if 'uom' not in df_main.columns:
                df_main['uom'] = 'undefined'

            if 'pps_point' not in df_main.columns:
                df_main['pps_point'] = 'undefined'
            
            if 'md_flow_name' not in df_main.columns:
                df_main['md_flow_name'] = ''                

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
            df_main['transaction_start_time'] = np.nan
            df_main['transaction_end_time'] = np.nan
            df_main['transaction_start_time'] = df_main.apply(
                lambda x: x['time'] if ( (x['event_name'] in ['transaction_start'] and x['flow_name'] in ['sdp','udp_stg','udp_non_stg_exp','udp_rc']) or  (x['event_name'] in ['rack_inducted'] and x['flow_name'] in ['udp_ind_exp']) or (x['event_name'] in ['carrier_scan'] and x['flow_name'] in ['udp_ind_inv']) ) else np.nan, axis=1)
            df_main['transaction_end_time'] = df_main.apply(
                lambda x: x['time'] if ( (x['event_name'] in ['send_msu'] and x['flow_name'] in ['udp_ind_exp','udp_stg','udp_non_stg_exp','udp_rc','udp_ind_inv']) or  (x['event_name'] in ['slot_scan_complete'] and x['flow_name'] in ['sdp']) ) else np.nan, axis=1)
            df_main['pps_mode'] = df_main['pps_mode'].apply(
                lambda x: x.replace('_mode', '') if not pd.isna(x) and len(x) > 4 else x)
            df_main['transaction_group'] = 0

            df_main = utilfunction.reset_index(df_main)
            df_main = self.apply_transaction_group(df_main)

            df_main = df_main.sort_values(
                by=['installation_id', 'pps_id', 'time', 'rack_id'],
                ascending=[True, True, True, True])
            df_main = df_main.groupby(
                ['installation_id', 'pps_id', 'pps_mode', 'transaction_group'],
                as_index=False).agg(
                time=('time', 'min'),
                rack_id=('rack_id', 'max'),
                operator_id=('operator_id', 'first'),
                station_type=('station_type','first'),
                storage_type=('storage_type','first'),
                transaction_start_time=('transaction_start_time', 'min'),
                transaction_end_time=('transaction_end_time', 'max'),
                time_end=('time', 'max'),
                slot_id = ('slot_id','max'),
                flow_name = ('flow_name','max'),
                md_flow_name = ('md_flow_name','max'))
            df_main = df_main[(~pd.isna(df_main['rack_id']))]
            df_main = df_main[~(pd.isna(df_main['transaction_end_time']))]
            df_main = df_main[~(pd.isna(df_main['transaction_start_time']))]

            if df_main.empty:
                return
            df_main = df_main.sort_values(
                by=['installation_id', 'pps_id', 'pps_mode', 'transaction_group', 'time', 'rack_id'],
                ascending=[True, True, True, True, True, True])
            df_main = utilfunction.reset_index(df_main)
            df_main['transaction_start_time_upper'] = df_main['transaction_start_time'].shift(-1)
            df_main['pps_id_upper'] = df_main['pps_id'].shift(-1)
            df_main['installation_id_upper'] = df_main['installation_id'].shift(-1)
            df_main['transaction_start_time_time_upper'] = df_main.apply(lambda x:x['transaction_start_time_upper'] if x['installation_id']==x['installation_id_upper'] and x['pps_id']==x['pps_id_upper'] else np.nan, axis=1)
            df_main['operator_working_time'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'transaction_end_time',
                                                        'transaction_start_time',
                                                        'operator_working_time')
            df_main['operator_waiting_time'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'transaction_start_time_time_upper',
                                                        'transaction_end_time',
                                                        'operator_waiting_time')
            df_main = utilfunction.datediff_in_millisec(df_main, 'time_end',
                                                            'transaction_end_time',
                                                            'operator_waiting_time')
            df_main["ntile"] = 0
            df_main=self.apply_ntile(df_main)
            df_main = df_main.groupby(
                ['installation_id', 'pps_id', 'pps_mode', 'ntile'],
                as_index=False).agg(
                time=('time', 'min'),
                rack_id=('rack_id', 'first'),
                station_type=('station_type','first'),
                storage_type=('storage_type','first'),
                operator_id=('operator_id', 'first'),
                transaction_start_time=('transaction_start_time','min'),
                transaction_end_time=('transaction_end_time', 'max'),
                operator_waiting_time=('operator_waiting_time', 'sum'),
                operator_working_time=('operator_working_time', 'sum'),
                slot_id = ('slot_id','max'),
                flow_name = ('flow_name','max'),
                md_flow_name = ('md_flow_name','max'))

            df_main['operator_waiting_time'] = df_main['operator_waiting_time'].astype(int, errors='ignore')
            df_main['operator_working_time'] = df_main['operator_working_time'].astype(int, errors='ignore')
            df_main['pps_id'] = df_main['pps_id'].astype(str, errors='ignore')
            df_main['rack_id'] = df_main['rack_id'].astype(str, errors='ignore')
            df_main['operator_id'] = df_main['operator_id'].astype(str, errors='ignore')

            df_main["start"] = pd.to_datetime(self.start_date) - timedelta(minutes=30)
            df_main["flag2"] = df_main.apply(self.date_compare, axis=1)
            df_main = df_main[(df_main["flag2"])]
            if not df_main.empty:
                df_main = df_main.drop(['flag2', 'start'], axis=1)

            df_main['face'] = df_main['slot_id'].apply(lambda x: x.split('.')[1] if (~pd.isna(x) and x!='undefined' and x!='') else 'undefined')
            if not df_main.empty:
                l = ['transaction_start_time_upper', 'pps_id_upper', 'installation_id_upper', 'prev_rack_id','transaction_group','time_end','index','ntile','start','slot_id']
                for col in l:
                    if col in df_main.columns:
                        del df_main[col]
            df_main.transaction_start_time = df_main.transaction_start_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
            df_main.transaction_end_time = df_main.transaction_end_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

            str_cols = ['installation_id','pps_mode', 'pps_id','station_type','storage_type','rack_id','transaction_start_time','operator_id','transaction_end_time','flow_name','face','md_flow_name']
            float_cols = ['operator_working_time','operator_waiting_time']
            df_main[float_cols] = df_main[float_cols].fillna(0)
            df_main = utilfunction.str_typecast(df_main,str_cols)
            df_main = utilfunction.float_typecast(df_main,float_cols)

            df_main.time = pd.to_datetime(df_main.time)
            df_main = df_main.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_main, "put_owt_summary",
                                          db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['pps_id'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None

# with DAG(
#         'put_owt_summary',
#         default_args=CommonFunction().get_default_args_for_dag(),
#         description='calculation of operator_working_time',
#         schedule_interval='55 * * * *',
#         max_active_runs=1,
#         max_active_tasks=16,
#         concurrency=16,
#         catchup=False
# ) as dag:
#     import csv
#     import os
#     import functools

#     if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
#         csvReader = CommonFunction().get_all_site_data_config()
#         for tenant in csvReader:
#             if tenant['Active'] == "Y" and tenant['operator_working_time'] == "Y":
#                 Put_owt_summary_final_task = PythonOperator(
#                     task_id='put_owt_summary_final_{}'.format(tenant['Name']),
#                     provide_context=True,
#                     python_callable=functools.partial(put_operator_working_time_summary().put_operator_working_time_summary,
#                                                       tenant_info={'tenant_info': tenant}),
#                     execution_timeout=timedelta(seconds=3600),
#                 )
#     else:
#         tenant = CommonFunction().get_tenant_info()
#         Put_owt_summary_final_task = PythonOperator(
#             task_id='put_owt_summary_final',
#             provide_context=True,
#             python_callable=functools.partial(put_operator_working_time_summary().put_operator_working_time_summary,
#                                               tenant_info={'tenant_info': tenant}),
#             op_kwargs={
#                 'tenant_info1': tenant,
#             },
#             execution_timeout=timedelta(seconds=3600),
#         )