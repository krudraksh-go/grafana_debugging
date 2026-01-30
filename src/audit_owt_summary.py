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
class AuditOperatorWorkingSummary:

    def audit_operator_working_time_summary(self, tenant_info, **kwargs):
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

        check_start_date = self.client.get_start_date("audit_owt_summary", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        # self.remove_unnecassary_tag_keys()

        q = f"select * from flow_transaction_events where time>'{check_start_date}' and time<='{check_end_date}'  and (event_name = 'transaction_start' or event_name = 'transaction_end') and (pps_mode='audit' or pps_mode='audit_mode') limit 1"
        df_flow_transaction_events = pd.DataFrame(self.read_client.query(q).get_points())
        if df_flow_transaction_events.empty:
            self.end_date = datetime.now(timezone.utc)
            self.audit_operator_working_time_summary1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("audit_owt_summary", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.audit_operator_working_time_summary1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def audit_operator_working_time_summary1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("audit_owt_summary", self.tenant_info)
        self.original_start_date = pd.to_datetime(self.start_date) - timedelta(minutes=30)
        self.original_start_date = self.original_start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start date: {self.start_date}, end_date : {self.end_date}, original_start_date : {self.original_start_date}")
        utilfunction = CommonFunction()

        q = f"select * from flow_transaction_events where time>'{self.start_date}' and time<='{self.end_date}'  and (event_name='transaction_start' or event_name='transaction_end')  and (pps_mode='audit' or pps_mode='audit_mode') limit 1"
        df_main = pd.DataFrame(self.read_client.query(q).get_points())
        if df_main.empty:
            filter = {"site": self.tenant_info['Name'], "table": "audit_owt_summary"}
            new_last_run = {"last_run": self.end_date}
            utilfunction.update_dag_last_run(filter, new_last_run)
            return
        q = f"select * from flow_transaction_events where time>'{self.original_start_date}' and time<='{self.end_date}' and (pps_mode='audit' or pps_mode='audit_mode') and (event_name = 'transaction_start' or event_name = 'transaction_end') order by time" 
                
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

            df_main = df_main.sort_values(by=['installation_id', 'pps_id', 'time'], ascending=[True, True, True])
            df_main = utilfunction.reset_index(df_main)

            ttp_setup = (self.tenant_info['is_ttp_setup'] == 'Y')
            df_main = utilfunction.update_station_type(df_main, ttp_setup)
            df_main = utilfunction.update_storage_type(df_main, ttp_setup)

            df_main['prev_installation_id'] = df_main['installation_id'].shift(1)
            df_main['prev_pps_id'] = df_main['pps_id'].shift(1)
            df_main['prev_event_name'] = df_main['event_name'].shift(1)
            df_main['prev_time'] = df_main['time'].shift(1)
            df_main = df_main[((df_main['event_name'] != df_main['prev_event_name']) | (
                                       df_main['pps_id'] != df_main['prev_pps_id']) | (
                                       df_main['installation_id'] != df_main['prev_installation_id']) )]
            
            df_main['time'] = pd.to_datetime(df_main['time'], utc=True)
            if 'operator_id' not in df_main.columns:
                df_main['operator_id'] = "default"

            df_main['value'] = np.nan
            df_main = utilfunction.datediff_in_millisec(df_main, 'time','prev_time','value')

            df_main['event_name'] = df_main['event_name'].apply(lambda x : 'waiting time' if x=='transaction_start' else 'working time')
            df_main = df_main[df_main['value']>0].reset_index(drop=True)
            
            df_main = df_main[['time','event_name','installation_id','operator_id','pps_id','rack_id','value']]
            str_cols = ['event_name','installation_id', 'pps_id','operator_id','rack_id']
            float_cols = ['value']
            df_main[float_cols] = df_main[float_cols].fillna(0)
            df_main = utilfunction.str_typecast(df_main,str_cols)
            df_main = utilfunction.float_typecast(df_main,float_cols)

            df_main.time = pd.to_datetime(df_main.time)
            df_main = df_main.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_main, "audit_owt_summary",
                                          db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['pps_id','event_name'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None
