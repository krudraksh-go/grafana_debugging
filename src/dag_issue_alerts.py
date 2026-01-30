## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from utils.CommonFunction import CommonFunction
import redis
import json
from utils.DagIssueAlertUtils import create_alert_html_tables
import pandas as pd

## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name =os.environ.get('Out_db_name', 'airflow')
sender = os.environ.get('ALERT_SENDER', 'analytics@greyorange.com')
receiver_list = os.environ.get('ALERT_RECEIVERS', ['analytics-team@greyorange.com'])


dag = DAG(
    'dag_issue_alerts',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Sends dag failure alerts periodically',
    schedule_interval = timedelta(hours=12),
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
)

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class DagIssueAlert:
    def get_error_type_keys(self):
        error_types = [
            'db_connection_failure',
            'influxdb_write_issue',
            'dag_failure'
        ]
        return error_types

    def check_and_send_alerts(self, **kwargs):
        redis_client = redis.StrictRedis(host='redis', port=6379, db=0)
        error_types = self.get_error_type_keys()
        json_strings_total = []
        for error_type in error_types:
            json_strings = redis_client.lrange(error_type, 0, -1)
            json_strings_total = json_strings_total + json_strings
        json_list_total = [json.loads(json_str) for json_str in json_strings_total]
        num_elements = len(json_list_total)

        if num_elements > 0:
            alert_html = create_alert_html_tables(json_list_total)
            CommonFunction().send_alert_mail(alert_html, sender, receiver_list)

        for error_type in error_types:
            json_strings = redis_client.lrange(error_type, 0, -1)
            num_elements = len(json_strings)
            redis_client.ltrim(error_type, num_elements, -1)

        return None


with DAG(
    'dag_issue_alerts',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Sends dag failure alerts periodically',
    schedule_interval = '0 6 * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import os
    import functools
    final_task = PythonOperator(
        task_id='dag_issue_alerts',
        provide_context=True,
        python_callable=functools.partial(DagIssueAlert().check_and_send_alerts),
        execution_timeout=timedelta(seconds=3600),
    )
