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
import json
from config import (
    MongoDbServer
    )
class ToteAssociationCalculator:

    def connect_to_mongodb(self):
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        collection_name = self.tenant_info['Name']+'_tote_association_incomplete_cycle'
        self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)

    def save_incomplete_cycle(self, df):
        df['time'] = df['time'].apply(lambda x: self.CommonFunction.convert_to_nanosec(x))
        if not df.empty:
            test = self.cls_mdb.delete_data({}, multiple=True)
            json_list = df.to_json(orient="records")
            json_list = json.loads(json_list)
            self.cls_mdb.insert_data(json_list)
    
    def get_incomplete_cycle(self):
        data = self.cls_mdb.get_two_days_data(self.start_date)
        df = pd.DataFrame(data)
        return df

    def convert_to_ns(self, df,col):
        df[col] = df[col].apply(lambda x: x.timestamp())
        df[col] = df[col]* 1e9
        return df

    def create_columns(self, df, col_list):
        for cols in col_list:
            df[cols] = np.nan
        return df

    def apply_group(self, df):
        Group=1
        for x in df.index:
            if x > 0:
                if df['installation_id'][x]==df['installation_id'][x-1] and df['pps_id'][x]==df['pps_id'][x-1] and df['bin_id'][x]==df['bin_id'][x-1] and df['event_name'][x]!='tote_association' :
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

        check_start_date = self.client.get_start_date("tote_association_time_differences", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select * from pick_fsm_events where time>'{check_start_date}' and time<='{check_end_date}' and seat_type = 'back' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("tote_association_time_differences", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("tote_association_time_differences", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
        
    def final_call1(self,end_date,**kwargs):
        self.start_date = self.client.get_start_date("tote_association_time_differences", self.tenant_info)
        self.utilfunction = CommonFunction()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}")

        q = f"select time, bin_id, event_name, fulfilment_area, host, installation_id, order_id, pps_id, seat_name, seat_type, tote_id, user_loggedin from pick_fsm_events where time>='{self.start_date}' and time<='{self.end_date}' and seat_type = 'back'"
        pick_fsm_events = pd.DataFrame(self.read_client.query(q).get_points())
        if pick_fsm_events.empty:
            filter = {"site": self.tenant_info['Name'], "table": "tote_association_time_differences"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)
            return   
        else:
            # if 'pps_id_1' in pick_fsm_events.columns:
            #     del pick_fsm_events['pps_id_1']
            initial_cols = pick_fsm_events.columns
            self.connect_to_mongodb()
            prev_incomplete_cycles = self.get_incomplete_cycle()
            self.cls_mdb.close_connection()
            if not prev_incomplete_cycles.empty:
                prev_incomplete_cycles = prev_incomplete_cycles[initial_cols]
                prev_incomplete_cycles = prev_incomplete_cycles.drop_duplicates(subset=['installation_id', 'pps_id', 'bin_id', 'event_name'], keep='last')
                pick_fsm_events = pd.concat([pick_fsm_events,prev_incomplete_cycles])
                pick_fsm_events = pick_fsm_events.reset_index()

            pick_fsm_events = pick_fsm_events.sort_values(by=['installation_id', 'pps_id','bin_id','time'], ascending=[True,True,True,True]).reset_index()
            pick_fsm_events['group']=0
            pick_fsm_events = self.apply_group(pick_fsm_events)
            pick_fsm_events['time'] = pd.to_datetime(pick_fsm_events['time'],utc=True)
            cols_to_create = ['tote_association', 'order_front_complete', 'tote_release_from_bin', 'back_order_clearing', 'order']
            pick_fsm_events = self.create_columns(pick_fsm_events, cols_to_create)
            pick_fsm_events['next_pps'] = pick_fsm_events['pps_id'].shift(-1)
            pick_fsm_events['next_bin'] = pick_fsm_events['bin_id'].shift(-1)

            pick_fsm_events['tote_association'] = pick_fsm_events.apply(
                lambda x: x['time'] if x['event_name'] == 'tote_association' else np.nan, axis=1)
            pick_fsm_events['order_front_complete'] = pick_fsm_events.apply(
                        lambda x: x['time'] if x['event_name'] == 'order_front_complete' else np.nan, axis=1)
            pick_fsm_events['tote_release_from_bin'] = pick_fsm_events.apply(
                        lambda x: x['time'] if x['event_name'] == 'tote_release_from_bin' else np.nan, axis=1)
            pick_fsm_events['back_order_clearing'] = pick_fsm_events.apply(
                        lambda x: x['time'] if x['event_name'] == 'back_order_clearing' else np.nan, axis=1)
            df = pick_fsm_events.groupby(['group','pps_id','bin_id'], as_index=False).agg(time=('time', 'min'),tote_association_time=('tote_association','min'),order_front_complete_time=('order_front_complete','min'),tote_release_from_bin_time=('tote_release_from_bin','min'),back_order_clearing_time=('back_order_clearing','min'), order = ('order_id','last'),next_pps_id = ('next_pps','last'), next_bin_id = ('next_bin','last'), seat_name = ('seat_name','max'),user_loggedin = ('user_loggedin','max') )
            df['next_tote_association_time']= df['tote_association_time'].shift(-1)
            completed_cycles = df[~pd.isna(df['tote_association_time']) & ~pd.isna(df['order_front_complete_time']) & ~pd.isna(df['back_order_clearing_time']) & ~pd.isna(df['tote_release_from_bin_time']) & ~pd.isna(df['next_tote_association_time']) & (df['pps_id']==df['next_pps_id']) & (df['bin_id']==df['next_bin_id'])].reset_index()
            if completed_cycles.empty:
                return
            incomplete_cycles = df[pd.isna(df['tote_association_time']) | pd.isna(df['order_front_complete_time']) | pd.isna(df['back_order_clearing_time']) | pd.isna(df['tote_release_from_bin_time']) |  pd.isna(df['next_tote_association_time']) | (df['pps_id']!=df['next_pps_id']) | (df['bin_id']!=df['next_bin_id'])].reset_index()
            incomplete_cycles = incomplete_cycles[['group']]
            cycles_not_complete = pd.merge(pick_fsm_events,incomplete_cycles,on=['group'],how='left')
            cycles_not_complete = cycles_not_complete[initial_cols]

            if not cycles_not_complete.empty:
                self.connect_to_mongodb()
                cycles_not_complete = cycles_not_complete.drop_duplicates(subset=['installation_id', 'pps_id','bin_id','event_name'], keep='last')
                self.save_incomplete_cycle(cycles_not_complete)
                self.cls_mdb.close_connection()

            cols = ['tote_association_time','order_front_complete_time','tote_release_from_bin_time','back_order_clearing_time','next_tote_association_time']
            for col in cols:
                completed_cycles = self.convert_to_ns(completed_cycles,col)
            
            if 'index' in completed_cycles.columns:
                del completed_cycles['index']
            if 'group' in completed_cycles.columns:
                del completed_cycles['group']
            completed_cycles['pps_id'] = completed_cycles['pps_id'].astype(str,errors='ignore')
            completed_cycles['bin_id'] = completed_cycles['bin_id'].astype(str,errors='ignore')
            completed_cycles['order'] = completed_cycles['order'].astype(str,errors='ignore')
            completed_cycles['seat_name'] = completed_cycles['seat_name'].astype(str,errors='ignore')
            completed_cycles['user_loggedin'] = completed_cycles['user_loggedin'].astype(str,errors='ignore')

            completed_cycles = completed_cycles.set_index('time')
            print("inserted")
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                    port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(completed_cycles, "tote_association_time_differences", db_name=self.tenant_info["alteryx_out_db_name"],
                                            tag_columns=['pps_id','bin_id'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None
# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'tote_association_time_calculation',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Time required to associate a tote',
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
            if tenant['Active'] == "Y" and tenant['tote_association_time_calculation'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='tote_association_time_calculation_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(ToteAssociationCalculator().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='tote_association_time_calculation_final',
            provide_context=True,
            python_callable=functools.partial(ToteAssociationCalculator().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )