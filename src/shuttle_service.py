import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
import os
from config import (
    CONFFILES_DIR
)


class shuttle_service:

    def apply_tote_cycle_group(self, df):
        group = 0
        collist = df.columns
        for x in df.index:
            col = f"{df['event_type'][x]}" + "_time"
            if col in collist:
                df[col][x] = df['time_ns'][x]

            if x >= 1:
                if df['order_id'][x] != df['order_id'][x - 1]:
                    group = group + 1
                    time_ns = df['time_ns'][x]
                df['complete_cycle'][x] = group
                df['time_diff'][x] = df['time_ns'][x] - time_ns
            else:
                time_ns = df['time_ns'][x]
        return df

    def final_call(self, tenant_info, **kwargs):
        self.CommonFunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["write_influx_ip"],
                                      port=self.tenant_info["write_influx_port"], db='PlatformBusinessStats')

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("shuttle_service_data", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        # self.end_date = datetime.now(timezone.utc)
        # self.final_call1(self.end_date, **kwargs)

        q = f"select *  from packing_box_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("shuttle_service_data", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("shuttle_service_data", '15min', self.tenant_info)
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
        self.original_start_date = self.client.get_start_date("shuttle_service_data", self.tenant_info)
        self.start_date = pd.to_datetime(self.original_start_date) - timedelta(minutes=60)
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        # prev_date = 
        self.utilfunction = CommonFunction()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}")

        new_col = ['packing_box_create_request_time', 'packing_box_created_time', 'packing_box_reached_eofc_time',
                   'shuttle_inbound_task_create_time',
                   'shuttle_inbound_task_completed_time', 'shuttle_outbound_task_completed_time',
                   'move_conveyor_back_to_front_create_time', 'initiate_pick_operations_time',
                   'shuttle_outbound_task_create_time', 'move_conveyor_back_to_front_completed_time',
                   'move_conveyor_front_to_back_create_time',
                   'move_conveyor_front_to_back_completed_time', 'carrier_not_required_inbound_task_create_time',
                   'carrier_not_required_time',
                   'carrier_not_required_inbound_task_completed_time',
                   'carrier_not_required_inbound_shift_task_create_time',
                   'carrier_not_required_inbound_shift_task_completed_time','shuttle_exception_time']
        # path = f'butler_nav_incidents/{self.tenant_info["Name"]}_shuttle_service.csv'
        # path = os.path.join(CONFFILES_DIR, path)
        # isExist = os.path.isfile(path)
        # if isExist:
        #     cof = pd.read_csv(path)
        # else:
        #     cof = pd.DataFrame()
        q = f"select * from packing_box_events where time >='{self.start_date}' and time<='{self.end_date}' order by time"
        shuttle_service_events = pd.DataFrame(self.read_client.query(q).get_points())
        if shuttle_service_events.empty:
            filter = {"site": self.tenant_info['Name'], "table": "shuttle_service_data"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return None
        # if not cof.empty:
        #     shuttle_service_events = pd.concat([shuttle_service_events, cof])

        shuttle_service_events = self.utilfunction.reset_index(shuttle_service_events)
        shuttle_service_events['time'] = pd.to_datetime(shuttle_service_events['time'], utc=True)
        shuttle_service_events['complete_cycle'] = 0
        shuttle_service_events['time_ns'] = 0
        shuttle_service_events['time_diff'] = 0
        for col in new_col:
            if col not in shuttle_service_events.columns:
                shuttle_service_events[col] = np.nan

        shuttle_service_events['time_ns'] = shuttle_service_events.time.apply(lambda x: int(pd.to_datetime(x).strftime('%s')))
        shuttle_service_events = shuttle_service_events.sort_values(by=['installation_id','order_id',  'time'], ascending=[True, True, True])
        shuttle_service_events = shuttle_service_events.reset_index()
        shuttle_service_events['prev_bin_id'] = shuttle_service_events['bin_id'].shift(1)
        shuttle_service_events['prev_component'] = shuttle_service_events['component'].shift(1)
        shuttle_service_events = self.apply_tote_cycle_group(shuttle_service_events)
        group_data = shuttle_service_events.groupby(['installation_id','order_id', 'complete_cycle'], as_index=False).agg(
            packing_box_create_request_time_min=('packing_box_create_request_time', 'min'),
            packing_box_created_time_min=('packing_box_created_time', 'min'),
            packing_box_reached_eofc_time_min=('packing_box_reached_eofc_time', 'min'),
            shuttle_inbound_task_create_time_min=('shuttle_inbound_task_create_time', 'min'),
            shuttle_inbound_task_create_time_max=('shuttle_inbound_task_create_time', 'max'),
            shuttle_inbound_task_completed_time_min=('shuttle_inbound_task_completed_time', 'min'),
            shuttle_inbound_task_completed_time_max=('shuttle_inbound_task_completed_time', 'max'),
            initiate_pick_operations_time_min=('initiate_pick_operations_time', 'min'),
            shuttle_outbound_task_create_time_min=('shuttle_outbound_task_create_time', 'min'),
            shuttle_outbound_task_create_time_max=('shuttle_outbound_task_create_time', 'max'),
            shuttle_outbound_task_completed_time_min=('shuttle_outbound_task_completed_time', 'min'),
            shuttle_outbound_task_completed_time_max=('shuttle_outbound_task_completed_time', 'max'),
            move_conveyor_back_to_front_create_time_min=('move_conveyor_back_to_front_create_time', 'min'),
            move_conveyor_back_to_front_completed_time_min=('move_conveyor_back_to_front_completed_time', 'min'),
            move_conveyor_front_to_back_create_time_min=('move_conveyor_front_to_back_create_time', 'min'),
            move_conveyor_front_to_back_completed_time_min=('move_conveyor_front_to_back_completed_time', 'min'),
            carrier_not_required_inbound_task_create_time_min=('carrier_not_required_inbound_task_create_time', 'min'),
            carrier_not_required_time_min=('carrier_not_required_time', 'min'),
            carrier_not_required_inbound_task_completed_time_min=('carrier_not_required_inbound_task_completed_time', 'min'),
            carrier_not_required_inbound_shift_task_create_time_min=('carrier_not_required_inbound_shift_task_create_time', 'min'),
            carrier_not_required_inbound_shift_task_completed_time_min=('carrier_not_required_inbound_shift_task_completed_time', 'min'),
            shuttle_exception_time_min=('shuttle_exception_time', 'min')
        )
        shuttle_service_events = pd.merge(shuttle_service_events, group_data, how='left', on=['installation_id','order_id', 'complete_cycle'])
        shuttle_service_events = shuttle_service_events[(shuttle_service_events['time'] >= self.original_start_date) | (~pd.isna(shuttle_service_events['packing_box_created_time_min']))].reset_index(drop=True)
        l=['complete_cycle', 'index'] + new_col
        for col in l:
            if col in shuttle_service_events.columns:
                del shuttle_service_events[col]

        shuttle_service_events.time = pd.to_datetime(shuttle_service_events.time)
        shuttle_service_events = shuttle_service_events.set_index('time')

        if not shuttle_service_events.empty:
                #shuttle_service_events['binId'] = shuttle_service_events['binId'].astype(str, errors='ignore')
                shuttle_service_events['bin_id'] = shuttle_service_events['bin_id'].astype(str, errors='ignore')
                shuttle_service_events['pps_id'] = shuttle_service_events['pps_id'].astype(str, errors='ignore')
                shuttle_service_events['shuttle_id'] = shuttle_service_events['shuttle_id'].astype(str, errors='ignore')
                shuttle_service_events['packing_box_create_request_time_min'] = shuttle_service_events['packing_box_create_request_time_min'].astype(float, errors='ignore')
                shuttle_service_events['packing_box_created_time_min'] = shuttle_service_events['packing_box_created_time_min'].astype(float, errors='ignore')
                shuttle_service_events['packing_box_reached_eofc_time_min'] = shuttle_service_events['packing_box_reached_eofc_time_min'].astype(float, errors='ignore')
                shuttle_service_events['shuttle_inbound_task_create_time_min'] = shuttle_service_events['shuttle_inbound_task_create_time_min'].astype(float, errors='ignore')
                shuttle_service_events['shuttle_inbound_task_create_time_max'] = shuttle_service_events['shuttle_inbound_task_create_time_max'].astype(float, errors='ignore')
                shuttle_service_events['shuttle_inbound_task_completed_time_min'] = shuttle_service_events['shuttle_inbound_task_completed_time_min'].astype(float, errors='ignore')
                shuttle_service_events['shuttle_inbound_task_completed_time_max'] = shuttle_service_events['shuttle_inbound_task_completed_time_max'].astype(float, errors='ignore')
                shuttle_service_events['initiate_pick_operations_time_min'] = shuttle_service_events['initiate_pick_operations_time_min'].astype(float, errors='ignore')
                shuttle_service_events['shuttle_outbound_task_create_time_min'] = shuttle_service_events['shuttle_outbound_task_create_time_min'].astype(float, errors='ignore')
                shuttle_service_events['shuttle_outbound_task_create_time_max'] = shuttle_service_events['shuttle_outbound_task_create_time_max'].astype(float, errors='ignore')
                shuttle_service_events['shuttle_outbound_task_completed_time_min'] = shuttle_service_events['shuttle_outbound_task_completed_time_min'].astype(float, errors='ignore')
                shuttle_service_events['shuttle_outbound_task_completed_time_max'] = shuttle_service_events['shuttle_outbound_task_completed_time_max'].astype(float, errors='ignore')
                shuttle_service_events['move_conveyor_back_to_front_create_time_min'] = shuttle_service_events['move_conveyor_back_to_front_create_time_min'].astype(float, errors='ignore')
                shuttle_service_events['move_conveyor_back_to_front_completed_time_min'] = shuttle_service_events['move_conveyor_back_to_front_completed_time_min'].astype(float, errors='ignore')
                shuttle_service_events['move_conveyor_front_to_back_create_time_min'] = shuttle_service_events['move_conveyor_front_to_back_create_time_min'].astype(float, errors='ignore')
                shuttle_service_events['move_conveyor_front_to_back_completed_time_min'] = shuttle_service_events['move_conveyor_front_to_back_completed_time_min'].astype(float, errors='ignore')
                shuttle_service_events['carrier_not_required_inbound_task_create_time_min'] = shuttle_service_events['carrier_not_required_inbound_task_create_time_min'].astype(float, errors='ignore')
                shuttle_service_events['carrier_not_required_time_min'] = shuttle_service_events['carrier_not_required_time_min'].astype(float, errors='ignore')
                shuttle_service_events['carrier_not_required_inbound_task_completed_time_min'] = shuttle_service_events['carrier_not_required_inbound_task_completed_time_min'].astype(float, errors='ignore')
                shuttle_service_events['carrier_not_required_inbound_shift_task_create_time_min'] = shuttle_service_events['carrier_not_required_inbound_shift_task_create_time_min'].astype(float,
                                                                                                         errors='ignore')
                shuttle_service_events['carrier_not_required_inbound_shift_task_completed_time_min'] = shuttle_service_events['carrier_not_required_inbound_shift_task_completed_time_min'].astype(float,
                                                                                                            errors='ignore')
                shuttle_service_events['shuttle_exception_time_min'] = shuttle_service_events['shuttle_exception_time_min'].astype(float, errors='ignore')
                if 'level_0' in shuttle_service_events.columns:
                    del shuttle_service_events['level_0']
                print("inserted")
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                     port=self.tenant_info["write_influx_port"])
                self.write_client.writepoints(shuttle_service_events, "shuttle_service_data",
                                              db_name=self.tenant_info["alteryx_out_db_name"],
                                              tag_columns=['shuttle_id','pps_id'], dag_name=os.path.basename(__file__),
                                              site_name=self.tenant_info['Name'])
        return None


# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
        'shuttle_service',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Shuttle Service',
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
            if tenant['Active'] == "Y" and tenant['shuttle_service'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='shuttle_service_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(shuttle_service().final_call,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='shuttle_service_final',
            provide_context=True,
            python_callable=functools.partial(shuttle_service().final_call, tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )