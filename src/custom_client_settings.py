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
class Client_setting:

    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        self.utilfunction = CommonFunction()
        df = self.utilfunction.get_sheet_from_airflow_setup_excel('client_custom_settings')
        site = self.tenant_info['Name']
        df = df[df['Name']==site]
        combine_df = pd.DataFrame()
        if not df.empty:
            df['key_value'] = df['key_value'].apply(ast.literal_eval)
            all_keys = set().union(*(d.keys() for d in df['key_value']))
            for key in all_keys:
                df[key] = df['key_value'].apply(lambda x: x.get(key, None))
            del df['key_value']
            for index, row in df.iterrows():
                df1_transposed = row.T
                #df1_transposed = df1_transposed.drop(df1_transposed.index[:1])
                final_df = df1_transposed.reset_index()
                final_df.columns = ['field_name', 'value']
                installion_id = final_df[final_df['field_name'] == 'installation_id']
                print(installion_id)
                if not installion_id.empty:
                    installion_id=installion_id.reset_index()
                    final_df['installion_id'] = installion_id['value'][0]
                final_df['installion_id'] = final_df['installion_id'].astype('str')
                final_df['date'] = '1/1/2024'
                combine_df=pd.concat([final_df,combine_df])
            combine_df=self.utilfunction.reset_index(combine_df)
            combine_df = combine_df.set_index(pd.to_datetime(np.arange(len(combine_df)), unit='s', origin=combine_df['date'][1]))
            del combine_df['date']
            self.client.query("drop measurement client_settings")
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(combine_df, "client_settings", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=['field_name'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'Client_setting',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Upload client setting from Config Sheet',
    schedule_interval = None,
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
            if tenant['Active'] == "Y" and tenant['Client_setting'] == "Y":
                Client_setting_final_task = PythonOperator(
                    task_id='Client_setting_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(Client_setting().final_call,tenant_info={'tenant_info': tenant}),
                )
                execution_timeout = timedelta(seconds=600),
    else:
        tenant = CommonFunction().get_tenant_info()
        Client_setting_final_task = PythonOperator(
            task_id='Client_setting_final',
            provide_context=True,
            python_callable=functools.partial(Client_setting().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )