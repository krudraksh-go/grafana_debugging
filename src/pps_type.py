## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient
import numpy as np
import pandas as pd
import ast
#from dags.utils.CommonFunction import InfluxData
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
from config import (
    rp_one_year
)

## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')



## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class PpsType:
    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        self.utilfunction = CommonFunction()
        df = self.utilfunction.get_sheet_from_airflow_setup_excel('pps_information')
        site = self.tenant_info['Name']
        df = df[df['Site'] == site]
        if not df.empty:
            df['pps_id'] = df['pps_id'].str.split(',')
            df = df.explode('pps_id')
            df = self.utilfunction.reset_index(df)
            if 'index' in df.columns:
                del df['index']
            
            curr_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            df['date'] = curr_date
            df = df.set_index(pd.to_datetime(np.arange(len(df)), unit='s', origin=df['date'][1]))
            del df['date']
            df['pps_id'] = df['pps_id'].astype(str,errors='ignore')
            df['pps_id'] = df['pps_id'].apply(lambda x: x.strip())
            if 'level_0' in df.columns:
                del df['level_0']
            df['host']= self.tenant_info['host_name']
            # df['installation_id'] = self.tenant_info['installation_id']
            df['host'] = df['host'].astype(str)
            df['installation_id'] = df['installation_id'].astype(str)
            self.client.query(f"drop measurement pps_information")
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df, "pps_information", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'PpsType',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Information of pps type',
    schedule_interval = '0 0 1 */3 *',
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
            if tenant['Active'] == "Y" and tenant['pps_type'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                PpsType_final_task = PythonOperator(
                    task_id='PpsType_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(PpsType().final_call,tenant_info={'tenant_info': tenant}),
                )
                execution_timeout = timedelta(seconds=600),
    else:
        tenant = CommonFunction().get_tenant_info()
        PpsTypeg_final_task = PythonOperator(
            task_id='PpsType_final',
            provide_context=True,
            python_callable=functools.partial(PpsType().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )