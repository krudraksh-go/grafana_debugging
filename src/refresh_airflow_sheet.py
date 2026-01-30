## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
from pandasql import sqldf

## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name =os.environ.get('Out_db_name', 'airflow')



dag = DAG(
    'refresh_airflow_google_sheet',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'refresh_airflow_google_sheet',
    schedule_interval = timedelta(days=1),
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
)

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class refresh_airflow_google_sheet:
    def refresh_airflow_google_sheet_final(self, **kwargs):
        CommonFunction().refresh_airflow_setup_sheet()
        return None


with DAG(
    'refresh_airflow_google_sheet',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'refresh_airflow_google_sheet',
    schedule_interval = '41 15 * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    final_task = PythonOperator(
        task_id='refresh_airflow_google_sheet',
        provide_context=True,
        python_callable=functools.partial(refresh_airflow_google_sheet().refresh_airflow_google_sheet_final),
        execution_timeout=timedelta(seconds=3600),
    )
