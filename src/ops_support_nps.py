import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction,postgres_connection
from pandasql import sqldf
import pytz
import time
import numpy as np
import os
import psycopg2



class OpsSupportNps():
    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        # self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        if self.tenant_info['Name']=='Coupang_Daegu' or self.tenant_info['Name']=='Dillards':
            self.connection = postgres_connection(database='platform_srms',
                                                         user=self.tenant_info['Postgres_pf_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_pf'], \
                                                         port=self.tenant_info['Postgres_pf_port'],
                                                         password=self.tenant_info['Postgres_pf_password'],
                                                         dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            
        else:
            self.connection = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_tower'], \
                                                         port=self.tenant_info['Postgres_tower_port'],
                                                         password=self.tenant_info['Tower_password'],
                                                         dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        


        cursor1 = self.connection.pg_curr
        cursor2 = self.connection.pg_curr
        cursor1.execute ("select created as time, scale, user_id from account_npstrackerdata")
        cursor2.execute ("select id, first_name, username from auth_user")
        NPS = cursor1.fetchall()
        Auth = cursor2.fetchall()
        df1=pd.DataFrame(NPS,columns=["time","Scale","User ID"])
        df2=pd.DataFrame(Auth, columns=["User ID","First name","User name"])
        dfgb=pd.merge(df1,df2,on="User ID",how = 'right')
        dfgb['Site'] = self.tenant_info['Name']
        df=pd.concat([dfgb])
        df['host'] = self.tenant_info['host_name']
        df['installation_id'] = self.tenant_info['installation_id']
        df['host'] = df['host'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)

        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')

        # print(f"df is {df}")

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "ops_support_nps", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=None, dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        self.connection.close()
        print("data inserted!!")


with DAG(
    'ops_support_nps',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'ops support nps',
    schedule_interval = '55 6 * * *',
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
            if tenant['Active'] == "Y" and tenant['ops_support_nps'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                ops_support_nps_final_task = PythonOperator(
                    task_id='ops_support_nps_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(OpsSupportNps().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":"dev_postgres", "influx_ip":"dev_influxdb", "influx_port":8086,\
        #           "write_influx_ip":"dev_influxdb","write_influx_port":8086, \
        #           "out_db_name":"airflow", "alteryx_out_db_name": "airflow"}
        tenant = CommonFunction().get_tenant_info()
        ops_support_nps_final_task = PythonOperator(
            task_id='ops_support_nps_final',
            provide_context=True,
            python_callable=functools.partial(OpsSupportNps().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )