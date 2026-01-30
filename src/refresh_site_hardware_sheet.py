## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator

from utils.CommonFunction import  CommonFunction

## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
dag = DAG(
    'refresh_site_hardware_detail',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'refresh daily site hardware detail sheet',
    schedule_interval = timedelta(days=1),
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
)

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class refresh_site_hardware_detail_sheet:
    def refresh_site_hardware_detail(self, **kwargs):
        CommonFunction().refresh_site_hardware_detail()
        return None

with DAG(
    'refresh_site_hardware_detail',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'refresh_site_hardware_detail_sheet',
    schedule_interval = '30 7 * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    final_task = PythonOperator(
        task_id='refresh_site_hardware_detail_sheet',
        provide_context=True,
        python_callable=functools.partial(refresh_site_hardware_detail_sheet().refresh_site_hardware_detail),
        execution_timeout=timedelta(seconds=3600),
    )
