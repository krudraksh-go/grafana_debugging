import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
import os
class mtu_dock_flow:
    def final_call(self,tenant_info,**kwargs):
        self.CommonFunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("mtu_dock_flow", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select * from mtu_flow_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("mtu_dock_flow", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self,end_date,**kwargs):
        self.start_date = self.client.get_start_date("mtu_dock_flow", self.tenant_info)
        self.utilfunction = CommonFunction()
        self.end_date = end_date
        self.start_date = (pd.to_datetime(self.start_date)-timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}")


        q = f"select * from mtu_flow_events where time>='{self.start_date}' and time<='{self.end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "mtu_dock_flow"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)       
        else:
            q = f"select * from mtu_flow_events where time>='{self.start_date}' and time<='{self.end_date}'"
            df = pd.DataFrame(self.read_client.query(q).get_points())
            df = df.sort_values(by=['installation_id', 'pps_id','dock_station_id','time'], ascending=[True,True,True,True]).reset_index()
            df['time']=pd.to_datetime(df['time'],utc=True)
            df['prev_event']= df['event'].shift(1)
            df['prev_time'] = df['time'].shift(1)
            df['prev_dock_station_id'] = df['dock_station_id'].shift(1)
            df['prev_installation_id'] = df['installation_id'].shift(1)
            df['time_diff'] = np.nan
            df['time_diff'] = df.apply(lambda x: (x['time']-x['prev_time']).total_seconds() if x['dock_station_id']==x['prev_dock_station_id'] and x['installation_id']==x['prev_installation_id'] else np.nan,axis=1)
            df['new_event'] = df['prev_event']+'_to_'+df['event']
            fil = ['time','installation_id','new_event','time_diff','dock_station_id','rack_id','pps_id']
            df = df[fil]
            df = df[ ~pd.isna(df['time_diff'])].reset_index()
            del df['index']
            df = df.rename(columns = {"new_event":"event"})
            df['time_diff'] = df['time_diff'].astype(float,errors='ignore')
            df = df.set_index('time')
            print("inserted")
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                    port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df, "mtu_dock_flow", db_name=self.tenant_info["alteryx_out_db_name"],
                                            tag_columns=['pps_id','dock_station_id'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None
# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'mtu_dock_flow',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'MTU flow Dock Undock time diff',
    schedule_interval = '56 * * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['mtu_dock_flow'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='mtu_dock_flow_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(mtu_dock_flow().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='mtu_dock_flow_final',
            provide_context=True,
            python_callable=functools.partial(mtu_dock_flow().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )