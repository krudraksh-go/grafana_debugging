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

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class ShuttleUpitmeCalculation:

    def date_compare(self, x):
        diff = (pd.to_datetime(x['start'])-pd.to_datetime(x['time'])).total_seconds()
        # diff = (x['start'].tz_localize(tz=pytz.UTC)-x['interval_start'].tz_localize(tz=pytz.UTC)).total_seconds()
        if diff <= 0:
            return True
        return False    

    def round_up_to_next_10_minutes(self, dt):
        if dt.second > 0 or dt.microsecond > 0:
            dt += timedelta(minutes=1)
        dt = dt.replace(second=0, microsecond=0)
        minutes_to_add = (10 - dt.minute % 10) % 10
        if minutes_to_add:
            dt += timedelta(minutes=minutes_to_add)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def uptime_calculation_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("shuttle_uptime", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from peripheral_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.uptime_calculation_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("shuttle_uptime", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.uptime_calculation_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def uptime_calculation_final1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("shuttle_uptime", self.tenant_info)
        original_start_date = pd.to_datetime(self.start_date) - timedelta(minutes=20)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(self.start_date, self.end_date, original_start_date)

        q = f"select * from peripheral_events where time>'{original_start_date}' and time <= '{self.end_date}' and component='shuttle' order by time desc"
        peripheral_events = pd.DataFrame(self.read_client.query(q).get_points())
        q = f"select * from ppstask_events where time>'{original_start_date}' and time <= '{self.end_date}' and (event='rack_started_to_depart_pps' or  value<550000 )  order by time desc"
        pps_taskevents = pd.DataFrame(self.read_client.query(q).get_points())

        if peripheral_events.empty or pps_taskevents.empty:
            return

        if 'previous_pps_state' in pps_taskevents.columns:
            pps_taskevents=pps_taskevents[(pps_taskevents['previous_pps_state']=='active')]

        if 'rack_presented' in pps_taskevents.columns:
            pps_taskevents=pps_taskevents[(pps_taskevents['rack_presented']=='yes') | (pps_taskevents['event']=='rack_started_to_depart_pps')]

        peripheral_events['pps_id'] = peripheral_events['pps_id'].astype(int).astype(str) 
        pps_taskevents['pps_id'] = pps_taskevents['pps_id'].astype(str)
        peripheral_events['pps_id'] = peripheral_events['pps_id'].apply(lambda x: x[2:])
        peripheral_events['time'] = pd.to_datetime(peripheral_events['time'])
        peripheral_events['event_type'] = peripheral_events['event_type'].apply(lambda x: 'healthy' if x=='healthy' else 'error')
        pps_taskevents['time'] = pd.to_datetime(pps_taskevents['time'])
        peripheral_events['time'] = peripheral_events['time'].apply(lambda x: self.round_up_to_next_10_minutes(x))
        pps_taskevents['time'] = pps_taskevents['time'].apply(lambda x: self.round_up_to_next_10_minutes(x))   
        pps_taskevents = pps_taskevents.groupby(['time','pps_id','host','installation_id'], as_index=False).agg(
            operator_uptime= ('value', 'sum'))
        peripheral_events = peripheral_events.groupby(['time','pps_id','shuttle_id','event_type'], as_index=False).agg(
            time_interval_s= ('time_interval_s', 'sum'))
        peripheral_events = peripheral_events.pivot_table(
                                index=['time', 'pps_id','shuttle_id'],
                                columns='event_type',
                                values='time_interval_s',
                                aggfunc='sum',
                                fill_value=0
                            ).reset_index()  
        for col in ['error','healthy']:
            if col not in peripheral_events.columns:
                peripheral_events[col] = 0
        peripheral_events['total'] = peripheral_events['error'] + peripheral_events['healthy']
        peripheral_events['hardware_uptime'] = peripheral_events.apply(lambda x: 600*x['healthy']/x['total'] if x['total']>0 else 0, axis=1) 
        peripheral_events = peripheral_events.groupby(['time','pps_id'], as_index=False).agg(
            hardware_uptime= ('hardware_uptime', 'min'))             
        df = pd.merge(peripheral_events,pps_taskevents,on=['time','pps_id'], how='outer')
        df = df.fillna(0)
        if df.empty:
            return
        df['operator_uptime'] = df['operator_uptime']/1000
        df['max_hardware_operator_uptime'] = df.apply(lambda x: max(x['hardware_uptime'],x['operator_uptime']), axis = 1)
        df["start"] = pd.to_datetime(self.start_date) - timedelta(minutes=10)
        df["flag2"] = df.apply(self.date_compare, axis=1) 
        df = df[(df["flag2"])] 
        if df.empty:      
            return
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')
        df['max_hardware_operator_uptime'] = df['max_hardware_operator_uptime'].apply(lambda x: min(x,600))
        cols = ['pps_id','hardware_uptime','operator_uptime','max_hardware_operator_uptime']
        df = df[cols]
        str_cols = ['pps_id']
        float_cols = ['hardware_uptime','operator_uptime','max_hardware_operator_uptime']
        df = self.utilfunction.str_typecast(df,str_cols)
        df = self.utilfunction.float_typecast(df,float_cols)        
        print("inserted")
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "shuttle_uptime", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['pps_id'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None


with DAG(
        'Shuttle_uptime_calculation',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of shuttle uptime',
        schedule_interval='21 * * * *',
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
            if tenant['Active'] == "Y" and tenant['shuttle_uptime'] == "Y":
                try:
                    final_task = PythonOperator(
                        task_id='shuttle_uptime_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(ShuttleUpitmeCalculation().uptime_calculation_final,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=600),
                    )
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='shuttle_uptime_final',
            provide_context=True,
            python_callable=functools.partial(ShuttleUpitmeCalculation().uptime_calculation_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

