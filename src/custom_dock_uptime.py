## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
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

from config import (
    rp_one_year
)

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class CustomDockUptime:

    def typecast_df(self, df):
        float_cols = []
        for col in float_cols:
            df[col] = df[col].fillna('').astype(float)
        str_cols = list(set(df.columns) - set(float_cols))
        for col in str_cols:
            df[col] = df[col].fillna('').astype(str)
        return df    

    def save_last_run(self):
        filter = {"site": self.tenant_info['Name'], "table": f"{rp_one_year}.custom_dock_uptime"}
        new_last_run = {"last_run": self.end_date}
        self.CommonFunction.update_dag_last_run(filter, new_last_run) 

    def write_data_to_influx(self, df):
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "custom_dock_uptime", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['pps_id','dock_station_id','host','installation_id'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy=rp_one_year)        
        print("Inserted!!")

    def custom_dock_uptime_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"]) 
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])                                                  
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        self.CommonFunction = CommonFunction()
        check_start_date = self.client.get_start_date(f"{rp_one_year}.custom_dock_uptime", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from dock_uptime where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.custom_dock_uptime_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3(f"{rp_one_year}.custom_dock_uptime", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3(f"{rp_one_year}.custom_dock_uptime", '5min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.custom_dock_uptime_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e
    


    def custom_dock_uptime_final1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date(f"{rp_one_year}.custom_dock_uptime", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.back_date = pd.to_datetime(self.start_date) - timedelta(minutes=5)
        self.back_date = self.back_date.strftime('%Y-%m-%dT%H:%M:%SZ')


        q = f"select * from dock_uptime where time>'{self.start_date}' and time <= '{self.end_date}' and Butler_ip ='{self.tenant_info['Butler_ip']}' order by time"
        dock_uptime = pd.DataFrame(self.read_client.query(q).get_points())
        if dock_uptime.empty:
            q = f"select * from dock_uptime where time>'{self.start_date}' and time <= '{self.end_date}' order by time"
            dock_uptime = pd.DataFrame(self.read_client.query(q).get_points())
        if not dock_uptime.empty:
            dock_uptime['host']= self.tenant_info['host_name']
            dock_uptime['installation_id'] = self.tenant_info['installation_id']             
            dock_uptime['nearest_5min'] = pd.to_datetime(dock_uptime['time'], format='%Y-%m-%dT%H:%M:%SZ')
            dock_uptime['nearest_5min'] = dock_uptime['nearest_5min'].apply(lambda x : x.replace(second=0))
            dock_uptime['nearest_5min'] = dock_uptime['nearest_5min'].apply(lambda x : x.replace(minute=x.minute - x.minute % 5).strftime("%Y-%m-%d %H:%M:%S"))
            
            q = f"select pps_id, status, mode, installation_id, host, front_logged_in from pps_data where time>'{self.back_date}' and time <= '{self.end_date}' order by time"
            pps_data = pd.DataFrame(self.read_client.query(q).get_points())
            if pps_data.empty:
                return None  
            pps_data['time'] = pd.to_datetime(pps_data['time'])
            pps_data['time'] = pps_data['time'].apply(lambda x : x.replace(second=0).strftime("%Y-%m-%d %H:%M:%S"))          
            pps_data = pps_data.rename(columns = {'time':'nearest_5min'})

            dock_uptime['pps_id'] = dock_uptime['pps_id'].astype(str)
            pps_data['pps_id'] = pps_data['pps_id'].astype(str)

            df = pd.merge(dock_uptime, pps_data, on=['pps_id', 'nearest_5min','installation_id','host'], how='left')
            ffill_cols = ['front_logged_in','status','mode']
            for col in ffill_cols:
                df[col] = df[col].fillna(method='ffill')
            df['is_pps_open'] = 'no'
            df['is_pps_open'] = df.apply(lambda x : 'yes' if x['status']=='open' and x['front_logged_in']=='true' else 'no', axis=1)
            
            cols_to_del = ['nearest_5min','pps_id_1','status','front_logged_in']    
                   
            df = self.typecast_df(df)
            for col in cols_to_del:
                if col in df.columns:
                    del df[col]
            self.write_data_to_influx(df)
        else:
            self.save_last_run()
            return None
        return None


with DAG(
        'custom_dock_uptime',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='custom_dock_uptime dag',
        schedule_interval='*/5 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(seconds=3600),
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['custom_dock_uptime'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='custom_dock_uptime_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(CustomDockUptime().custom_dock_uptime_final,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=3600),
                    )
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='custom_dock_uptime_final',
            provide_context=True,
            python_callable=functools.partial(CustomDockUptime().custom_dock_uptime_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

