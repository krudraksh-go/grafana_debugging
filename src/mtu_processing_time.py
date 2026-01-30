## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------
#
import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import numpy as np

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class mtu_processing_time:

    def apply_transaction_group(self, df):
        group = 0
        df = df.sort_values(by=['dock_station_id', 'group_id', 'pps_id','rack_id','time'], ascending=[False, False, False,False,True])
        df = df.reset_index()
        for x in df.index:
            if x > 0:
                if df['dock_station_id'][x] == df['dock_station_id'][x - 1] and df['group_id'][x] == df['group_id'][
                    x - 1] and df['pps_id'][x] == df['pps_id'][x - 1] and df['rack_id'][x] == df['rack_id'][x - 1]:
                    df['transaction_group'][x] = group
                    if df['event'][x] == 'undock_complete':
                        group = group + 1
                else:
                    group = group + 1
                    df['transaction_group'][x] = group
        return df

    def apply_mtu_flow_group(self, df):
        group = 0
        df = df.sort_values(by=['packing_box_id', 'time'], ascending=[False,  True])
        df = df.reset_index()
        for x in df.index:
            if x > 0:
                if df['packing_box_id'][x] == df['packing_box_id'][x - 1]:
                    df['mtu_flow_group'][x] = group
                    if df['order_allocated_type'][x] == 'extracted':
                        group = group + 1
                else:
                    group = group + 1
                    df['mtu_flow_group'][x] = group
        return df
    def get_last_null_record_mtu_data(self):
        threshold_time = pd.to_datetime(self.start_date)
        threshold_time_2d = pd.to_datetime(self.start_date)-timedelta(days=2)
        try:
            mtu_proc_query = f"SELECT * FROM mtu_processing_time WHERE time >='{threshold_time_2d}' and mtu_cycle !=1 order by time asc limit 1"
            mtu_result = pd.DataFrame(self.read_client.query(mtu_proc_query).get_points())
            if mtu_result.empty:
                time_of_result = threshold_time
            else:
                time_of_result = pd.to_datetime(mtu_result['time'][0])
            return time_of_result
        except Exception as ex:
            print(f"error1 {ex}")
            return threshold_time

    def mtu_processing_time_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("mtu_processing_time", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.mtu_processing_time_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("mtu_processing_time", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.mtu_processing_time_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def mtu_processing_time_final1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("mtu_processing_time", self.tenant_info)
        self.start_date = self.get_last_null_record_mtu_data()
        self.end_date = end_date
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        self.read_client_platformbusinessstats = InfluxData(host=self.tenant_info["influx_ip"],
                                                            port=self.tenant_info["influx_port"],
                                                            db="PlatformBusinessStats")

        try:
            self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        except Exception as ex:
            print(f"error {ex}")
            pass
        print(f"start_date: {self.start_date}, end date: {self.end_date}")
        q = f"select * from item_picked where time>'{self.start_date}' and time <= '{self.end_date}' order by time desc"
        q2 = f"select * from mtu_flow_events where time>'{self.start_date}' and time <= '{self.end_date}' and (event ='dock_complete' or event ='undock_complete')   order by time desc"
        q3 = f"select mtu_id,order_id_field,packing_box_id,packing_box_type,order_allocated_type,mtu_status,order_status from mtu_data_platform where time>'{self.start_date}' and time <= '{self.end_date}'  order by time desc"
        q4 = f"select order_id, external_service_request_id, pick_before_time, event_name,installation_id  from order_events where time>'{self.start_date}' and time <= '{self.end_date}'  and (event_name='order_created' or event_name='order_tasked') order by time desc"
        df_item_picked = pd.DataFrame(self.read_client.query(q).get_points())
        df_mtu_flow_events = pd.DataFrame(self.read_client.query(q2).get_points())
        df_order_events = pd.DataFrame(self.read_client.query(q4).get_points())
        if not df_order_events.empty:
            df_order_created = df_order_events[df_order_events['event_name'] == 'order_created']
            df_order_created['order_creation_time'] = df_order_created['time']
            df_order_tasked = df_order_events[df_order_events['event_name'] == 'order_tasked'].rename(columns={'time': 'order_alloc_time'})
            #df_order_events = pd.merge(df_order_events, df_order_tasked[['order_id', 'order_alloc_time']], how='left', on='order_id')
            #df_order_events = df_order_events.drop(columns=['external_service_request_id'])
            df_order_events = pd.merge(df_order_created, df_order_tasked[['installation_id','order_id', 'order_alloc_time']], how='left', on=['installation_id','order_id'])
            df_order_events = df_order_events.drop_duplicates(subset=['installation_id','order_id'], keep='first')
        df_mtu_data_platform = pd.DataFrame(self.read_client_platformbusinessstats.query(q3).get_points())
        if not df_item_picked.empty and not df_mtu_data_platform.empty and not df_order_events.empty and not df_mtu_flow_events.empty:
            df_item_picked = df_item_picked.groupby(['installation_id', 'pps_id', 'order_id'], as_index=False).agg(
                time=('time', 'min'), pick_start_time=('time', 'min'),
                pick_complete_time=('time', 'max'))
            df_mtu_flow_events['transaction_group'] = 0
            df_mtu_data_platform['mtu_flow_group'] = 0
            df_mtu_flow_events = self.apply_transaction_group(df_mtu_flow_events)
            df_mtu_data_platform["packing_box_id"]=df_mtu_data_platform["packing_box_id"].astype('str')
            df_mtu_data_platform= self.apply_mtu_flow_group(df_mtu_data_platform)
            df_mtu_flow_events_dock = df_mtu_flow_events[df_mtu_flow_events['event'] == 'dock_complete']
            df_mtu_flow_events_undock = df_mtu_flow_events[df_mtu_flow_events['event'] == 'undock_complete']
            df_mtu_flow_events_dock_undock=pd.merge(df_mtu_flow_events_dock,df_mtu_flow_events_undock,  how='left', on=['transaction_group','dock_station_id','group_id','pps_id','rack_id'])
            df_mtu_flow_events_dock_undock.rename(columns={'time_x': 'Pick_MTU_dock_time','time_y': 'Pick_MTU_undock_time'}, inplace=True)
            df_mtu_flow_events_dock_undock=df_mtu_flow_events_dock_undock[['pps_id','rack_id','Pick_MTU_dock_time','Pick_MTU_undock_time']]
            #            df_item_picked = pd.merge(df_item_picked, df_mtu_data_platform, how='left', left_on=['order_id'], right_on=['order_id_field'])
            #            df_item_picked.rename(columns={'time_x': 'time','packing_box_id':'LPN_ID'}, inplace=True)
            df_extraction_docked = df_mtu_data_platform[
                df_mtu_data_platform['order_allocated_type'] == 'extraction_docked'].rename(
                columns={'time': 'extraction_dock_time', 'packing_box_id': 'LPN_ID'})
            df_extracted = df_mtu_data_platform[df_mtu_data_platform['order_allocated_type'] == 'extracted'].rename(
                columns={'time': 'extraction_undock_time'})
            # df_carton_induction = df_mtu_data_platform[df_mtu_data_platform['order_allocated_type'] == 'final'].rename(
            #     columns={'time': 'carton_induction_time'})
            df_carton_induction = df_mtu_data_platform[(df_mtu_data_platform['mtu_status'] == 'container_associated')].rename(
                columns={'time': 'carton_induction_time'})
            df_carton_induction = df_carton_induction.groupby(['mtu_flow_group'], as_index=False).agg(carton_induction_time=('carton_induction_time', 'max'),mtu_id=('mtu_id','first'),packing_box_type=('packing_box_type','first'))
            df_item_picked = pd.merge(df_item_picked, df_extraction_docked[['order_id_field', 'extraction_dock_time','LPN_ID','mtu_flow_group']],
                                      how='left', left_on='order_id', right_on='order_id_field')
            df_item_picked = pd.merge(df_item_picked, df_extracted[['order_id_field', 'extraction_undock_time']],
                                      how='left', left_on='order_id', right_on='order_id_field')
            df_item_picked = pd.merge(df_item_picked, df_carton_induction, how='left', left_on='mtu_flow_group',
                                      right_on='mtu_flow_group')
            df_item_picked = df_item_picked.drop_duplicates(subset=['order_id'], keep='first')
            df_item_picked = df_item_picked.drop(columns=['order_id_field_x','order_id_field_y'])
            df_item_picked = pd.merge(df_order_events[['time','order_id','order_creation_time','order_alloc_time','external_service_request_id','pick_before_time']], df_item_picked, how='left', on=['order_id'])
            df_item_picked.rename(columns={'time_x': 'time'}, inplace=True)
            df_item_picked.drop(columns=['time_y'], inplace=True)
            df_item_picked = pd.merge(df_item_picked, df_mtu_flow_events_dock_undock,
                                      how='left', left_on=['pps_id','mtu_id'], right_on=['pps_id','rack_id'])

            df_item_picked["temp_time_cal"] = np.nan
            df_item_picked = self.utilfunction.datediff_in_sec(df_item_picked, "Pick_MTU_dock_time",
                                                               "carton_induction_time", "temp_time_cal")
            df_item_picked_1 = df_item_picked[pd.isna(df_item_picked['temp_time_cal'])]
            df_item_picked=df_item_picked[df_item_picked['temp_time_cal']>0]
            temp_data = df_item_picked.groupby(['installation_id', 'pps_id', 'order_id'], as_index=False).agg(
                temp_time_cal1=('temp_time_cal', 'min'))
            df_item_picked=pd.merge(df_item_picked,temp_data ,how='inner' ,on=['installation_id', 'pps_id', 'order_id'])
            df_item_picked=df_item_picked[df_item_picked['temp_time_cal']==df_item_picked['temp_time_cal1']]
            del df_item_picked['temp_time_cal1']
            df_item_picked = pd.concat([df_item_picked, df_item_picked_1], axis=0)

            # df_item_picked.rename(
            #     columns={'time_y': 'Pick_MTU_dock_time', 'time_x': 'time'}, inplace=True)
            # df_item_picked = df_item_picked[
            #     df_item_picked['carton_induction_time'] <= df_item_picked['Pick_MTU_dock_time']]
            # df_item_picked = df_item_picked[
            #     df_item_picked['carton_induction_time'] < df_item_picked['extraction_dock_time']]
            df_item_picked["total_time_seconds"] = np.nan
            df_item_picked= df_item_picked.reset_index()
            df_item_picked = self.utilfunction.datediff_in_sec(df_item_picked, "extraction_dock_time",
                                                               "carton_induction_time", "total_time_seconds")

            # df_item_picked = pd.merge(df_item_picked, df_mtu_flow_events_undock[['pps_id', 'rack_id', 'time']],
            #                           how='left', left_on=['pps_id', 'mtu_id'], right_on=['pps_id', 'rack_id'])
            # df_item_picked.rename(
            #     columns={'time_y': 'Pick_MTU_undock_time', 'time_x': 'time'}, inplace=True)
            # df_item_picked = df_item_picked[
            #     df_item_picked['Pick_MTU_dock_time'] <= df_item_picked['Pick_MTU_undock_time']]
            # df_item_picked = df_item_picked[
            #     df_item_picked['pick_complete_time'] <= df_item_picked['Pick_MTU_undock_time']]

            # check later #df_item_picked = df_item_picked.drop(columns=['rack_id_x', 'rack_id_y', 'order_id_field'])
            df_item_picked['packing_box_type'] = df_item_picked['packing_box_type'].astype('str')
            df_item_picked['external_service_request_id'] = df_item_picked['external_service_request_id'].astype('str')
            df_item_picked['LPN_ID'] = df_item_picked['LPN_ID'].astype('str')
            df_item_picked['order_id'] = df_item_picked['order_id'].astype('str')
            df_item_picked['pps_id'] = df_item_picked['pps_id'].astype('str')
            df_item_picked['mtu_id'] = df_item_picked['mtu_id'].astype('str')
            df_item_picked['total_time_seconds'] = df_item_picked['total_time_seconds'].astype('float')
            df_item_picked['mtu_cycle'] = 0
            if not df_item_picked.empty:
                df_item_picked.mtu_cycle = df_item_picked.apply(lambda x: x.mtu_cycle if pd.isna(x.extraction_undock_time) else 1, axis=1)
            df_item_picked.time = pd.to_datetime(df_item_picked.order_creation_time,utc=True)
            df_item_picked = df_item_picked.set_index('time')
            if not df_item_picked.empty:
                l = ['temp_time_cal', 'temp_time_cal1', 'mtu_status', 'order_status', 'order_id_field','rack_id','packing_box_id','mtu_flow_group','index']
                for col in l:
                    if col in df_item_picked.columns:
                        del df_item_picked[col]
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_item_picked, "mtu_processing_time",
                                          db_name=self.tenant_info["out_db_name"], tag_columns=['mtu_id'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        else:
            filter = {"site": self.tenant_info['Name'], "table": "mtu_processing_time"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)       
        return None


with DAG(
        'mtu_processing_time',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of picks per rack face GM-44025',
        schedule_interval='2,17,32,57 * * * *',
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
            if tenant['Active'] == "Y" and tenant['mtu_processing_time'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                final_task = PythonOperator(
                    task_id='mtu_processing_time_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(mtu_processing_time().mtu_processing_time_final,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='mtu_processing_time_final',
            provide_context=True,
            python_callable=functools.partial(mtu_processing_time().mtu_processing_time_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

