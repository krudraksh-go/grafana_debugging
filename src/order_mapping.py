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

# Today=time.strftime('%Y-%m-%d')
# Kal=datetime.today() - timedelta(days=1)
# Parso=datetime.today() - timedelta(days=2)
# Yesterday=Kal.strftime('%Y-%m-%d')
# dby=Parso.strftime('%Y-%m-%d')



class OrderMapping():
    def final_call(self, tenant_info, **kwargs):
        # print("in final_call")
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        try:
            daterange = self.client.get_datetime_interval3("order_mapping", '1d', self.tenant_info)
            # print("daterange calculated")
            for i in daterange.index:
                self.end_date = daterange['end_date'][i]
                # print(self.end_date)
                self.final_call1(self.end_date, **kwargs)
            # self.end_date= datetime.now(timezone.utc)
            # self.final_call1(self.end_date,**kwargs)
        except Exception as e:
            print(f"error:{e}")

    def final_call1(self, end_date, **kwargs):

        self.start_date = self.client.get_start_date("order_mapping", self.tenant_info)
        # self.start_date='2023-08-20'
        self.end_date = end_date
        # self.end_date = self.end_date.replace(second=0,microsecond=0)
        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%d')
        self.start_date=pd.to_datetime(self.start_date)
        self.start_date=self.start_date.strftime('%Y-%m-%d')
        
        # print("trying to connect postrges!!")
        connection = postgres_connection(database='platform_srms',
                                                         user=self.tenant_info['Postgres_pf_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_pf'], \
                                                         port=self.tenant_info['Postgres_pf_port'],
                                                         password=self.tenant_info['Postgres_pf_password'],
                                                         dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        client_cursor = connection.pg_curr

        client_cursor.execute("select count(*) from service_request where created_on>='"+self.start_date+"' AND created_on<'"+self.end_date+"' and type='PICK'")
        totalcount = client_cursor.fetchall()
        # print("connected to postgres and total count is calculated!!")
        f=True
        time_offset = 0
        l = 50000
        tday = datetime.today()

        while f==True:
            if time_offset<=totalcount[0][0]:
                client_cursor.execute("select id, external_service_request_id as externalRequestId from service_request where created_on>='"+self.start_date+"' AND created_on<'"+self.end_date+"' and type='PICK' order by created_on offset "+str(time_offset)+" limit "+str(l)+"")
                batch = client_cursor.fetchall()

                df = pd.DataFrame(batch,columns=['id','externalRequestId'])

                print(f"df is {df}")
                
                #posting batch to influx
                
                dte = str(tday).split(' ')[0]
                df['date'] = dte
                df['cron_ran_at'] = time.strftime('%Y-%m-%d')
                df = df.set_index(pd.to_datetime(np.arange(len(df)),unit='s',origin=df['date'][1]))
                df.drop(['date'], axis = 1,inplace=True)
                print(f"df before write : {df}")
                df['host'] = self.tenant_info['host_name']
                df['installation_id'] = self.tenant_info['installation_id']
                df['host'] = df['host'].astype(str)
                df['installation_id'] = df['installation_id'].astype(str)
                # client = DataFrameClient(host=str(influxip), port=str(influxport), username='gor', password='Grey()orange')
                # client.write_points(df,'order_mapping',database='Alteryx',protocol='line',time_precision='s')
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
                self.write_client.writepoints(df, "order_mapping", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=None, dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

                print('Data inserted')
                
                time_offset = time_offset+50000
                tday = tday + timedelta(days=1)
            else:
                f=False
        connection.close()
        return None


with DAG(
    'order_mapping',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'orders mapping',
    schedule_interval = '50 6 * * *',
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
            if tenant['Active'] == "Y" and tenant['order_mapping'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                order_mapping_final_task = PythonOperator(
                    task_id='order_mapping_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(OrderMapping().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":"dev_postgres", "influx_ip":"dev_influxdb", "influx_port":8086,\
        #           "write_influx_ip":"dev_influxdb","write_influx_port":8086, \
        #           "out_db_name":"airflow", "alteryx_out_db_name": "airflow"}
        tenant = CommonFunction().get_tenant_info()
        order_mapping_final_task = PythonOperator(
            task_id='order_mapping_final',
            provide_context=True,
            python_callable=functools.partial(OrderMapping().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )
