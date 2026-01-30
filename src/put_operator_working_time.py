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
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
# from pandasql import sqldf
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
class PutOperatorWorkingTime:

    def date_compare(self, x):
        diff = (x['start'] - x['time']).total_seconds()
        if diff <= 0:
            return True
        return False
    


    def apply_transaction_group(self, df):
        group = 0
        for x in df.index - 1:
            if x >= 0:
                if (df['event_name'][x] in ['send_msu'] and df['flow_name'][x] in ['udp_stg','udp_ind_exp','udp_ind_inv','udp_rc','udp_non_stg_exp']) or (df['event_name'][x] == 'all_scan_complete' and df['flow_name'][x] in ['sdp']) or (df['pps_id'][x] != df['pps_id'][x + 1]) or (
                        df['installation_id'][x] != df['installation_id'][x + 1]):
                    group = group + 1
                df['transaction_group'][x + 1] = group
        return df

    def final_call(self, tenant_info, **kwargs):
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

        check_start_date = self.client.get_start_date("put_operator_working_time", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from flow_transaction_events where time>'{check_start_date}' and time<='{check_end_date}' and (pps_mode='put' or pps_mode = 'put_mode') and ((event_name='slot_scan_complete' and flow_name='sdp') or (event_name='send_msu' and flow_name='udp_ind_exp')) limit 1"
        df_flow_transaction_events = pd.DataFrame(self.read_client.query(q).get_points())
        if df_flow_transaction_events.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("put_operator_working_time", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("put_operator_working_time", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
    
    def remove_duplicate_rows(self, df):
        cols_to_create = ['prev_pps_id','prev_event_name','next_event_name','prev_installation_id','prev_butler_movement','prev_rack_id']
        df = CommonFunction().create_cols_in_df(df, cols_to_create)
        df['prev_pps_id'] = df['pps_id'].shift(1)
        df['prev_event_name'] = df['event_name'].shift(1)
        df['next_event_name'] = df['event_name'].shift(-1)
        df['prev_installation_id'] = df['installation_id'].shift(1)
        df['prev_butler_movement'] = df['butler_movement'].shift(1)
        df['prev_rack_id'] = df['rack_id'].shift(1)

        filter_criteria = ( (df['prev_pps_id']!=df['pps_id']) | (df['prev_event_name']!=df['event_name']) | (df['next_event_name']!=df['event_name']) |
                            (df['prev_installation_id']!=df['installation_id']) | (df['prev_butler_movement']!=df['butler_movement']) | (df['prev_rack_id']!=df['rack_id'])  )
        df = df[filter_criteria].reset_index(drop=True)

        double_filter = ( (df['prev_pps_id']!=df['pps_id']) | (df['prev_event_name']!=df['event_name']) | (df['event_name'] == 'system_idle_start') |
                            (df['prev_installation_id']!=df['installation_id']) | (df['prev_butler_movement']!=df['butler_movement']) | (df['prev_rack_id']!=df['rack_id'])  )
        df = df[double_filter].reset_index(drop=True)

        for col in cols_to_create:
            if col in df.columns:
                del df[col]
        return df

    def final_call1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("put_operator_working_time", self.tenant_info)
        self.original_start_date = pd.to_datetime(self.start_date) + timedelta(minutes=1)
        temp_date = pd.to_datetime(self.start_date) - timedelta(minutes=10)
        self.start_date = pd.to_datetime(self.start_date)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        utilfunction = CommonFunction()
        print(f"start date: {self.start_date}, end_date : {self.end_date}, temp_date : {temp_date}")        

        q = f"select * from flow_transaction_events where time>'{self.original_start_date}' and time<='{self.end_date}'  and ((event_name='slot_scan_complete' and flow_name='sdp') or (event_name='send_msu')) limit 1"
        df_main = pd.DataFrame(self.read_client.query(q).get_points())
        if df_main.empty:
            filter = {"site": self.tenant_info['Name'], "table": "put_operator_working_time"} 
            new_last_run = {"last_run": self.end_date}
            utilfunction.update_dag_last_run(filter,new_last_run)  
            return

        q = f"select * from flow_transaction_events where time>'{temp_date}' and time<='{self.end_date}' and (pps_mode='put' or pps_mode='put_mode') order by time"
        df_main = pd.DataFrame(self.read_client.query(q).get_points())
        desired_events = [
                        ['carrier_scan','udp_ind_inv'],['send_msu','udp_ind_inv'],['exception_start','udp_ind_inv'],['exception_end','udp_ind_inv'],
                        ['slot_scan_complete','udp_ind_exp'],['carrier_scan','udp_ind_exp'],['rack_inducted','udp_ind_exp'],['scan_events','udp_ind_exp'],['send_msu','udp_ind_exp'],['exception_start','udp_ind_exp'],['exception_end','udp_ind_exp'],
                        ['transaction_start','udp_stg'],['slot_scan_complete','udp_stg'],['send_msu','udp_stg'],['scan_events','udp_stg'],['exception_start','udp_stg'],['exception_end','udp_stg'],
                        ['transaction_start','sdp'],['slot_scan_complete','sdp'],['all_scan_complete','sdp'],['exception_start','sdp'],['exception_end','sdp'],
                        ['transaction_start','udp_rc'],['scan_events','udp_rc'],['slot_scan_complete','udp_rc'],['send_msu','udp_rc'],['roll_cage_undocked','udp_rc'],['exception_start','udp_rc'],['exception_end','udp_rc'],
                        ['transaction_start','udp_non_stg_exp'],['scan_events','udp_non_stg_exp'],['slot_scan_complete','udp_non_stg_exp'],['send_msu','udp_non_stg_exp'],['exception_start','udp_non_stg_exp'],['exception_end','udp_non_stg_exp'],
        ]
        df_main = df_main[df_main.apply(lambda x: [x['event_name'], x['flow_name']] in desired_events, axis=1)].reset_index(drop=True) 

        if df_main.empty:
            return
        if not df_main.empty:
            df_main = df_main.fillna('')
            if 'carrier_type' in df_main.columns:
                df_main = df_main[df_main['carrier_type'] != 'packing_box']

            if 'uom' not in df_main.columns:
                df_main['uom'] = 'undefined'
            
            if 'md_flow_name' not in df_main.columns:
                df_main['md_flow_name'] = ''

            df_main = df_main.sort_values(by=['installation_id', 'pps_id', 'time'], ascending=[True, True, True])
            df_main = df_main.reset_index(drop=True)
            df_main = self.remove_duplicate_rows(df_main)

            ttp_setup = (self.tenant_info['is_ttp_setup']=='Y')
            df_main = utilfunction.update_station_type(df_main,ttp_setup)
            df_main = utilfunction.update_storage_type(df_main,ttp_setup)

            df_main['time'] = pd.to_datetime(df_main['time'], utc=True)
            if 'operator_id' not in df_main.columns:
                df_main['operator_id'] = "default"
            
            df_main['prev_event_name'] = df_main['event_name'].shift(1)
            df_main['prev_pps_id'] = df_main['pps_id'].shift(1)
            df_main['prev_installation_id'] = df_main['installation_id'].shift(1)
            df_main['prev_flow_name'] = df_main['flow_name'].shift(1)
            df_main['prev_time'] = df_main['time'].shift(1)
            df_main['time_diff'] = df_main.apply(lambda x: (x['time']-x['prev_time']).total_seconds() * 1000, axis=1)
            df_main = df_main[(df_main['prev_installation_id'] == df_main['installation_id']) & (df_main['prev_pps_id'] == df_main['pps_id'])].reset_index(drop=True)
            del_cols = ['prev_pps_id','prev_installation_id','prev_flow_name','prev_time']
            for col in del_cols:
                if col in df_main.columns:
                    del df_main[col]
            
            # prev_event_name, event_name, flow_name, new_event_name
            new_event = [
                # udp_stg
                ['transaction_start', 'scan_events','udp_stg', 'item_scan_time'],
                ['scan_events', 'slot_scan_complete','udp_stg', 'item_put_time'],
                ['slot_scan_complete', 'scan_events','udp_stg', 'item_scan_time'],
                ['slot_scan_complete', 'send_msu','udp_stg', 'time_to_send_msu'],
                ['scan_events','send_msu','udp_stg', 'time_to_send_msu'],
                ['exception_start','exception_end','udp_stg', 'time_to_raise_exception'],

                # udp_ind_exp
                ['send_msu','carrier_scan','udp_ind_exp', 'staging_time'],
                ['send_msu','rack_inducted','udp_ind_exp', 'tote_indcution_time'],
                ['carrier_scan','rack_inducted','udp_ind_exp', 'tote_indcution_time'],
                ['rack_inducted','scan_events','udp_ind_exp', 'item_scan_time'],
                ['scan_events','slot_scan_complete','udp_ind_exp','item_put_time'],
                ['slot_scan_complete','send_msu','udp_ind_exp', 'time_to_send_msu'],
                ['exception_start','exception_end','udp_ind_exp', 'time_to_raise_exception'],

                # udp_ind_inv
                ['send_msu','carrier_scan','udp_ind_inv', 'staging_time'],
                ['exception_start','exception_end','udp_ind_inv', 'time_to_raise_exception'],


                # sdp
                ['transaction_start', 'all_scan_complete','sdp', 'item_scan_time'],
                ['all_scan_complete', 'slot_scan_complete','sdp', 'item_put_time'],
                ['exception_start','exception_end','sdp', 'time_to_raise_exception'],


                # udp_rc
                ['slot_scan_complete','roll_cage_undocked','udp_rc', 'rollcage_undocked_time'],
                ['transaction_start', 'scan_events','udp_rc', 'item_scan_time'],
                ['scan_events', 'slot_scan_complete','udp_rc', 'item_put_time'],
                ['slot_scan_complete', 'scan_events','udp_rc', 'item_scan_time'],
                ['exception_start','exception_end','udp_rc', 'time_to_raise_exception'],


                # udp_non_stg_exp
                ['transaction_start', 'scan_events','udp_non_stg_exp', 'item_scan_time'],
                ['scan_events', 'slot_scan_complete','udp_non_stg_exp', 'item_put_time'],
                ['slot_scan_complete', 'transaction_start','udp_non_stg_exp', 'item_scan_time'],
                ['exception_start','exception_end','udp_non_stg_exp', 'time_to_raise_exception'],
            ]

            new_events_map = pd.DataFrame(new_event, columns = ['prev_event_name','event_name','flow_name','new_event_name'])
            df_main = pd.merge(df_main,new_events_map, on = ['prev_event_name','event_name','flow_name'], how = 'left')
            df = df_main[~pd.isna(df_main['new_event_name'])].reset_index(drop=True)
            if df.empty:
                return

            df['transaction_group'] = 0
            df = self.apply_transaction_group(df)
            df['rack_id'] = df['rack_id'].apply(lambda x: '' if x == 'undefined' or pd.isna(x) else x)
            
            agg_dict = {
                'time': ('time', 'min'),
                'rack_id': ('rack_id', 'last'),
                'uom_type': ('uom', 'last'),
                'operator_id': ('operator_id', 'first'),
                'flow_name': ('flow_name', 'last'),
                'md_flow_name': ('md_flow_name', 'last'),
                'transaction' : ('transaction','sum'),
                'station_type' : ('station_type','first'),
                'storage_type' : ('storage_type','first'),
            }
            cols = df['new_event_name'].unique()
            for col in cols:
                df[col] = 0
                df[col] = df.apply(lambda x: x['time_diff'] if x['new_event_name'] == col else 0, axis=1)
                agg_dict[col] = (col, 'sum')
            df['transaction'] = 0
            df['transaction'] = df['new_event_name'].apply(lambda x: 1 if x=='item_scan_time' else 0) 
            df = df.groupby(['installation_id', 'pps_id','transaction_group',],as_index=False).agg(**agg_dict)           

            df["start"] = pd.to_datetime(self.start_date) - timedelta(minutes=5)
            df["flag2"] = df.apply(self.date_compare, axis=1)
            df = df[(df["flag2"])]    
            if not df.empty:
                df = df.drop(['flag2', 'start'], axis=1)                
                
            df = utilfunction.reset_index(df)
            del_cols = ['index','transaction_group']
            for col in del_cols:
                if col in df.columns:
                    del df[col]
            df = df.set_index('time')
            str_cols = ['installation_id','pps_mode', 'pps_id','station_type','storage_type','rack_id','uom_type','operator_id','face','sku_id','flow_name','md_flow_name']
            float_cols = ['item_put_time','item_scan_time','staging_time','tote_indcution_time','rollcage_undocked_time','transaction_count','time_to_send_msu','time_to_raise_exception']
            df = utilfunction.str_typecast(df,str_cols)
            self.df = utilfunction.float_typecast(df,float_cols)

            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df, "put_operator_working_time",
                                          db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['pps_id', 'rack_id'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            print("inserted")

        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
        'put_operator_working_time',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of operator_working_time for put',
        schedule_interval = '*/15 * * * *',
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
            if tenant['Active'] == "Y" and tenant['put_operator_working_time'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='put_operator_working_time_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(PutOperatorWorkingTime().final_call,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='put_operator_working_time_final',
            provide_context=True,
            python_callable=functools.partial(PutOperatorWorkingTime().final_call,
                                              tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )