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
db_name =os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class put_events:

    def put_events_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
#        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("put_events_airflow", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from put_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.put_events_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("put_events_airflow", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.put_events_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def put_events_final1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("put_events_airflow", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select * from put_events where time>'{self.start_date}' and time <= '{self.end_date}' order by time desc"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if not df.empty:
            df.put_quantity = df.put_quantity.apply(lambda x: int(x) if not pd.isna(x) else 0)
            df.put_quantity = df.put_quantity.astype(int)
            df.time = pd.to_datetime(df.time)
            df = df.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df, "put_events_airflow", db_name=self.tenant_info["out_db_name"],tag_columns=['pps_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        else:
            filter = {"site": self.tenant_info['Name'], "table": "put_events_airflow"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
        return None


with DAG(
    'put_events',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of picks per rack face GM-44025',
    schedule_interval = '@hourly',
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
            if tenant['Active'] == "Y" and tenant['put_events'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                final_task = PythonOperator(
                    task_id='put_events_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(put_events().put_events_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='put_events_final',
            provide_context=True,
            python_callable=functools.partial(put_events().put_events_final,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

