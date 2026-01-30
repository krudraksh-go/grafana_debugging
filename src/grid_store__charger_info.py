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

class grid_store_charger_info:
    def grid_store_charger_info(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.influxclient = self.client.influx_client
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        self.butler = Butler_api(host=self.tenant_info['Butler_ip'], port=self.tenant_info['Butler_port'], host_name = self.tenant_info['host_name'])
        ppsinfo_data=self.butler.fetch_ppsinfo_data()
        gridinfo_data=self.butler.fetch_gridinfo_data()
        storageinfo_data = self.butler.fetch_storageinfo_data()
        chargerinfo_data = self.butler.fetch_chargerinfo_data()
        tote_data = self.butler.fetch_toteinfo_data()
        tote_data2 = self.butler.fetch_toteinfo_data2()
        self.influxclient.query("drop measurement pps_info")
        self.client.write_points(jsondatavalue=ppsinfo_data, database=self.tenant_info["alteryx_out_db_name"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        self.influxclient.query("drop measurement grid_info")
        self.client.write_points(jsondatavalue=gridinfo_data, database=self.tenant_info["alteryx_out_db_name"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        self.influxclient.query("drop measurement storage_info")
        self.client.write_points(jsondatavalue=storageinfo_data, database=self.tenant_info["alteryx_out_db_name"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        self.influxclient.query("drop measurement charger_info")
        self.client.write_points(jsondatavalue=chargerinfo_data, database=self.tenant_info["alteryx_out_db_name"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        self.influxclient.query("drop measurement tote_info")
        self.client.write_points(jsondatavalue=tote_data, database=self.tenant_info["alteryx_out_db_name"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        self.influxclient.query("drop measurement tote_info2")
        self.client.write_points(jsondatavalue=tote_data2, database=self.tenant_info["alteryx_out_db_name"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'grid_store_charger_info',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of grid_store_charger_info',
    schedule_interval = '47 6 * * *',
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
            if tenant['Active'] == "Y" and tenant['grid_store_charger_info'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                grid_store_charger_info_final_task = PythonOperator(
                    task_id='grid_store_charger_info_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(grid_store_charger_info().grid_store_charger_info,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=60),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        grid_store_charger_info_final_task = PythonOperator(
            task_id='grid_store_charger_info_final',
            provide_context=True,
            python_callable=functools.partial(grid_store_charger_info().grid_store_charger_info,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

