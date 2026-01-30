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

#from dags.utils.CommonFunction import InfluxData
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
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
class Insight_suggestion:

    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        self.utilfunction = CommonFunction()
        df = self.utilfunction.get_sheet_from_airflow_setup_excel('insight_suggestion')
        suggestions= df[['Insight','SuggestionOnBreach','Parameter']]
        suggestions['date'] = '1/1/2024'
        suggestions = suggestions.set_index(pd.to_datetime(np.arange(len(suggestions)), unit='s', origin=suggestions['date'][1]))
        del suggestions['date']

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(suggestions, "suggestion", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=['Insight'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])


# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'Insight_suggestion',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Upload Insight Suggestion from Config Sheet',
    schedule_interval = "10 7 * * *",
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
            if tenant['Active'] == "Y" and tenant['Insight_suggestion'] == "Y":
                Insight_suggestion_final_task = PythonOperator(
                    task_id='Insight_suggestion_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(Insight_suggestion().final_call,tenant_info={'tenant_info': tenant}),
                )
                execution_timeout = timedelta(seconds=600),
    else:
        tenant = CommonFunction().get_tenant_info()
        Insight_suggestion_final_task = PythonOperator(
            task_id='Insight_suggestion_final',
            provide_context=True,
            python_callable=functools.partial(Insight_suggestion().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )


