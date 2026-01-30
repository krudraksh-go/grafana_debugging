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
from utils.CommonFunction import  MongoDBManager
from config import (
    CONFFILES_DIR,
    GOOGLE_BUCKET_NAME,
    MongoDbServer
)


class mtu_pick_pass_journey:

    def add_leg_information(self, df):
        group = 0
        for x in df.index:
            if x >= 1:
                if (df['mtu_life_cycle_uuid'][x] == df['mtu_life_cycle_uuid'][x - 1]) and (df['state'][x] == 'partially_filled'):
                    df['leg'][x] = 2
                elif (df['mtu_life_cycle_uuid'][x] == df['mtu_life_cycle_uuid'][x - 1]):
                    df['leg'][x] = df['leg'][x - 1]
        return df

    def final_call(self, tenant_info, **kwargs):
        self.CommonFunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["write_influx_ip"],
                                      port=self.tenant_info["write_influx_port"], db="PlatformBusinessStats")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("mtu_pick_pass_journey", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        # self.end_date = datetime.now(timezone.utc)
        # self.final_call1(self.end_date, **kwargs)

        q = f"select *  from MTU_Events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        #print(df)
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("mtu_pick_pass_journey", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("mtu_pick_pass_journey", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self, end_date, **kwargs):
        # self.start_date = '2023-12-26T17:00:37.532902628Z'
        self.start_date = self.client.get_start_date("mtu_pick_pass_journey", self.tenant_info)
        #self.start_date = pd.to_datetime(self.start_date) - timedelta(hours=4)
        self.utilfunction = CommonFunction()
        self.gcs = GCS()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}")


        path = f'butler_nav_incidents/{self.tenant_info["Name"]}_mtu_pick_pass_journey.csv'
        # CONFFILES_DIR='/home/ankush.j/dev/gm_data_flow/dags/'
        path = os.path.join(CONFFILES_DIR, path)
        source_blob_name = f'{self.tenant_info["Name"]}_mtu_pick_pass_journey.csv'
        self.gcs.download_from_bucket(GOOGLE_BUCKET_NAME, source_blob_name, path)
        isExist = os.path.isfile(path)
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        collection_name = source_blob_name.replace(".csv",'')
        self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
        selected_columns = ['time', 'event_type', 'mtu_id', 'mtu_life_cycle_uuid','pps_id', 'pps_type','state','installation_id']

        new_col = ['leg1_dock_tasked_time', 'leg1_dock_time', 'leg1_undock_time',
                   'leg2_dock_tasked_time', 'leg2_dock_time', 'leg2_undock_time',
                   'extraction_dock_tasked_time', 'extraction_dock_time', 'extraction_undock_time']
        data = self.cls_mdb.get_two_days_data(self.start_date)
        print(f"collection_name:{collection_name}")
        #print(data)
        if data:
            cof = pd.DataFrame(data)
            cof['time'] = pd.to_datetime(cof['time'], unit='ms')
            print("mongodb")
            #print(cof.sha)
        elif isExist:
            cof = pd.read_csv(path)
        else:
            cof = pd.DataFrame(columns=selected_columns)

        q = f"select * from MTU_Events where time >='{self.start_date}' and time<='{self.end_date}' order by time"
        mtu_events = pd.DataFrame(self.read_client.query(q).get_points())
        if mtu_events.empty:
            filter = {"site": self.tenant_info['Name'], "table": "mtu_pick_pass_journey"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return None
        if not cof.empty:
            mtu_events = pd.concat([mtu_events, cof])
        mtu_events = mtu_events[selected_columns]
        mtu_events = self.utilfunction.reset_index(mtu_events)
        mtu_events['time'] = pd.to_datetime(mtu_events['time'], utc=True)
        mtu_events['mtu_id'] = mtu_events['mtu_id'].astype(int, errors='ignore')
        mtu_events['pps_id'] = mtu_events['pps_id'].astype(str, errors='ignore')
        mtu_events = mtu_events.sort_values(by=['installation_id','mtu_life_cycle_uuid', 'time'],
                                                            ascending=[True, True, True])
        mtu_events = self.utilfunction.reset_index(mtu_events)
        mtu_events = mtu_events.drop_duplicates(subset=list(set(selected_columns) - set('time')), keep='first')
        mtu_events = self.utilfunction.reset_index(mtu_events)

        # mtu_events = mtu_events.sort_values(by=['installation_id', 'mtu_life_cycle_uuid', 'time'],
        #                                                     ascending=[True, True, True])
        # mtu_events = self.utilfunction.reset_index(mtu_events)
        # mtu_events = pd.concat([mtu_events, cof])
        mtu_events = mtu_events.sort_values(by=[ 'installation_id', 'mtu_life_cycle_uuid', 'time'], ascending=[True, True, True])
        mtu_events = self.utilfunction.reset_index(mtu_events)

        mtu_events['prev_event_type'] = mtu_events['event_type'].shift(1)
        mtu_events['prev_time'] = mtu_events['time'].shift(1)
        mtu_events['time_diff'] = np.nan
        mtu_events['leg'] = 1
        mtu_events = self.add_leg_information(mtu_events)
        mtu_events = self.utilfunction.datediff_in_millisec(mtu_events, 'time', 'prev_time', 'time_diff')
        mtu_events['time_ns'] = mtu_events.time.apply(lambda x: float(pd.to_datetime(x).strftime('%s')))
        group_data = mtu_events.groupby(['installation_id', 'mtu_id', 'mtu_life_cycle_uuid'], as_index=False).agg(
                                        max_leg=('leg', 'max'),
                                        cycle_start_time=('time_ns', 'min'))
        group_data['pick_n_pass_flag']=0
        group_data['pick_n_pass_flag'] = group_data['max_leg'].apply(lambda x:1 if x>1 else 0 )
        del group_data['max_leg']
        mtu_events = pd.merge(mtu_events, group_data, how='left',  on=['installation_id','mtu_id', 'mtu_life_cycle_uuid'])

        mtu_events_complete_cycle = mtu_events[
            (mtu_events['event_type'] == 'UNDOCKED') & (mtu_events['pps_type'] == 'extraction')]
        mtu_events_complete_cycle = mtu_events_complete_cycle[[ 'mtu_life_cycle_uuid']]
        mtu_events_complete_cycle['complete_cycle'] = 1
        mtu_events = pd.merge(mtu_events, mtu_events_complete_cycle, how='left', on=['mtu_life_cycle_uuid'])
        mtu_events['complete_cycle'] = mtu_events['complete_cycle'].apply(lambda x: 0 if x <= 0 or pd.isna(x) else x)
        mtu_events_incomplete_cycle = mtu_events[mtu_events['complete_cycle'] == 0]
        mtu_events_incomplete_cycle = mtu_events_incomplete_cycle[selected_columns]
        mtu_events = mtu_events[mtu_events['complete_cycle'] == 1]

        if mtu_events.empty:
            filter = {"site": self.tenant_info['Name'], "table": "mtu_pick_pass_journey"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            #mtu_events_incomplete_cycle.to_csv(path, index=False, header=True)
            #self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
            if self.cls_mdb.delete_data({},multiple=True):
                    if not mtu_events_incomplete_cycle.empty:
                        json_list = mtu_events_incomplete_cycle.to_json(orient="records")
                        #print(json_list)
                        json_list = json.loads(json_list)
                        test = self.cls_mdb.insert_data(json_list)
                    else:
                        mtu_events_incomplete_cycle.to_csv(path, index=False, header=True)
                        self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
                    #print(test)
            return None

        for col in new_col:
            if col not in mtu_events.columns:
                mtu_events[col] = np.nan
       # path1 = f'/home/ankush.j/dev/gm_data_flow/dags/{self.tenant_info["Name"]}_mtu_pick_pass_journey1.csv'
        # path = os.path.join(CONFFILES_DIR, path)
        #mtu_events.to_csv(path1, index=False, header=True)
        mtu_events['leg1_dock_tasked_time'] = mtu_events.apply(lambda x: x['time_ns'] -x['cycle_start_time'] if x['event_type'] == 'DOCK_TASK_CREATED'
                                        and x['state'] == 'container_associated'  and x['pps_type'] == 'pick' else np.nan, axis=1)
        mtu_events['leg1_dock_time'] = mtu_events.apply(lambda x: x['time_ns']-x['cycle_start_time']  if x['event_type'] == 'DOCKED'
                                        and x['state'] == 'container_associated' and x['pps_type'] == 'pick' else np.nan, axis=1)
        mtu_events['leg1_undock_time'] = mtu_events.apply(lambda x: x['time_ns']-x['cycle_start_time']  if x['event_type'] == 'UNDOCKED'
                                        and ((x['state'] == 'partially_filled' and x['pick_n_pass_flag']==1) or (x['state'] == 'filled' and x['pick_n_pass_flag']==0)) and x['pps_type'] == 'pick' else np.nan, axis=1)

        mtu_events['leg2_dock_tasked_time'] = mtu_events.apply(lambda x: x['time_ns'] -x['cycle_start_time'] if x['event_type'] == 'DOCK_TASK_CREATED'
                                        and x['state'] == 'partially_filled' and x['pps_type'] == 'pick' else np.nan, axis=1)
        mtu_events['leg2_dock_time'] = mtu_events.apply(lambda x: x['time_ns'] -x['cycle_start_time'] if x['event_type'] == 'DOCKED'
                                        and x['state'] == 'partially_filled' and x['pps_type'] == 'pick' else np.nan, axis=1)
        mtu_events['leg2_undock_time'] = mtu_events.apply(lambda x: x['time_ns']-x['cycle_start_time']  if x['event_type'] == 'UNDOCKED'
                                        and x['state'] == 'filled' and x['pps_type'] == 'pick' else np.nan, axis=1)
        mtu_events['extraction_dock_tasked_time'] = mtu_events.apply(lambda x: x['time_ns'] -x['cycle_start_time'] if x['event_type'] == 'DOCK_TASK_CREATED'
                                        and x['pps_type'] == 'extraction' else np.nan, axis=1)
        mtu_events['extraction_dock_time'] = mtu_events.apply(lambda x: x['time_ns']-x['cycle_start_time']  if x['event_type'] == 'DOCKED'
                                        and x['pps_type'] == 'extraction' else np.nan, axis=1)
        mtu_events['extraction_undock_time'] = mtu_events.apply(lambda x: x['time_ns']-x['cycle_start_time']  if x['event_type'] == 'UNDOCKED'
                                        and x['pps_type'] == 'extraction' else np.nan, axis=1)

        group_data = mtu_events.groupby(['installation_id', 'mtu_id', 'mtu_life_cycle_uuid'], as_index=False).agg(
            leg1_dock_tasked_time=('leg1_dock_tasked_time', 'max'),
            leg1_dock_time=('leg1_dock_time', 'max'),
            leg1_undock_time=('leg1_undock_time', 'max'),
            leg2_dock_tasked_time=('leg2_dock_tasked_time', 'max'),
            leg2_dock_time=('leg2_dock_time', 'max'),
            leg2_undock_time=('leg2_undock_time', 'max'),
            extraction_dock_tasked_time=('extraction_dock_tasked_time', 'max'),
            extraction_dock_time=('extraction_dock_time', 'max'),
            extraction_undock_time=('extraction_undock_time', 'max'),
        )
        mtu_events = mtu_events.drop(columns=['leg1_dock_tasked_time', 'leg1_dock_time','leg1_undock_time','leg2_dock_tasked_time',
                                              'leg2_dock_time','leg2_undock_time','extraction_dock_tasked_time','extraction_dock_time','extraction_undock_time'])

        mtu_events = pd.merge(mtu_events, group_data, how='left', on=['installation_id','mtu_id', 'mtu_life_cycle_uuid'])
        mtu_events.time = pd.to_datetime(mtu_events.time)
        mtu_events = mtu_events.set_index('time')
        if not mtu_events.empty:
                l = ['level_0', 'index', 'prev_time', 'complete_cycle']
                for col in l:
                    if col in mtu_events.columns:
                        del mtu_events[col]
                print("inserted")
                mtu_events['time_diff']=mtu_events['time_diff'].apply(lambda x:0 if x<0 else x)
                mtu_events['time_diff'] = mtu_events['time_diff'].astype(float, errors='ignore')
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                     port=self.tenant_info["write_influx_port"])
                self.write_client.writepoints(mtu_events, "mtu_pick_pass_journey",
                                              db_name=self.tenant_info["alteryx_out_db_name"],
                                              tag_columns=['pps_id','mtu_id'], dag_name=os.path.basename(__file__),
                                              site_name=self.tenant_info['Name'])
                #mtu_events_incomplete_cycle.to_csv(path, index=False, header=True)
                print("mtu_events_incomplete_cycle")
                if self.cls_mdb.delete_data({},multiple=True):
                    if not mtu_events_incomplete_cycle.empty:
                        json_list = mtu_events_incomplete_cycle.to_json(orient="records")
                        #print(json_list)
                        json_list = json.loads(json_list)
                        test = self.cls_mdb.insert_data(json_list)
                    else:
                        mtu_events_incomplete_cycle.to_csv(path, index=False, header=True)
                        self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
                    #print(test)
                #self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
                # if not mtu_events_incomplete_cycle.empty and not mtu_events.empty:
                #     mtu_events_incomplete_cycle.to_csv(path, index=False, header=True)
        return None


# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
        'mtu_pick_pass_journey',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Mtu Pick Pass cycle ',
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
            if tenant['Active'] == "Y" and tenant['mtu_pick_pass_journey'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='mtu_pick_pass_journey_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(mtu_pick_pass_journey().final_call,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='mtu_pick_pass_journey_final',
            provide_context=True,
            python_callable=functools.partial(mtu_pick_pass_journey().final_call, tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )