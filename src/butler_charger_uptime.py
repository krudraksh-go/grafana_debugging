## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, Butler_api
from pandasql import sqldf
import time

## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')



# dag = DAG(
#     'cell_voltages',
#     default_args = default_args,
#     description = 'calculation of cell_voltages GM-44036',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class butler_charger_uptime:
    def butler_charger_uptime(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        self.butler = Butler_api(host=self.tenant_info['Butler_ip'], port=self.tenant_info['Butler_port'])
        butler_uptime=self.butler.fetch_butleruptime_data()
        charger_uptime=self.butler.fetch_chargeruptime_data()
        self.client.write_points(jsondatavalue=butler_uptime, database=self.tenant_info["out_db_name"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        self.client.write_points(jsondatavalue=charger_uptime, database=self.tenant_info["out_db_name"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'butler_charger_uptime_only_for_on_cloud_sites',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of butler_charger_uptime',
    schedule_interval = '*/1 * * * * ',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        print("only valid for cloud sites")
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['streaming'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                butler_charger_uptime_final_task = PythonOperator(
                    task_id='butler_charger_uptime_final{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(butler_charger_uptime().butler_charger_uptime,
                                                        tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        butler_charger_uptime_final_task = PythonOperator(
            task_id='butler_charger_uptime_final',
            provide_context=True,
            python_callable=functools.partial(butler_charger_uptime().butler_charger_uptime,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

