import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction,postgres_connection
from pandasql import sqldf
import pytz
import time
import numpy as np
import os


class TotalOrdersCreated():

    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        try:
            daterange = self.client.get_datetime_interval3("total_orders_created", '1d', self.tenant_info)
            for i in daterange.index:
                self.end_date = daterange['end_date'][i]
                self.final_call1(self.end_date, **kwargs)
        except AirflowTaskTimeout as timeout_exception:
            raise timeout_exception                 
        except Exception as e:
            print(f"error:{e}")
            raise e

    def final_call1(self, end_date, **kwargs):
        self.end_date = end_date
        self.start_date = self.client.get_start_date("total_orders_created", self.tenant_info)
        self.start_date=pd.to_datetime(self.start_date)
        self.start_date=self.start_date.strftime('%Y-%m-%d')
        self.end_date = self.end_date.strftime('%Y-%m-%d')
        start_time=self.tenant_info['StartTime']
        start = self.start_date+" "+start_time
        end = self.end_date+" "+start_time

        self.connection = postgres_connection(database='platform_srms',
                                                         user=self.tenant_info['Postgres_pf_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_pf'], \
                                                         port=self.tenant_info['Postgres_pf_port'],
                                                         password=self.tenant_info['Postgres_pf_password'],
                                                         dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        client_cursor = self.connection.pg_curr
        client_cursor.execute("select ((service_request.attributes::jsonb)->'order_options'->'bintags'->1->0 ) "
                    "as bintags,sum(stock_unit.quantity) as total_items_created, count(service_request.id) as total_orders_created "
                    " from service_request , service_request_children , service_request_expectations , container_stock_units , stock_unit where service_request .id = service_request_children.service_request_id "
                    "and service_request_children.servicerequests_id = service_request_expectations.service_request_id and "
                    "service_request_expectations.expectations_id = container_stock_units.container_id and container_stock_units.stockunits_id = "
                    "stock_unit.id and service_request.created_on>'%s' and "
                    "service_request.created_on<'%s' and service_request.type='PICK'group by bintags" %(start,end))
        
        Orders_created = client_cursor.fetchall()
        self.connection.close()
        df2 = pd.DataFrame(Orders_created, columns=['bin_tags','total_items_created','total_orders_created'])
        if not df2.empty:
            df2['time']= self.end_date
            df2['Site']=self.tenant_info["Name"]
            df2['host'] = self.tenant_info['host_name']
            df2['installation_id'] = self.tenant_info['installation_id']
            df2['host'] = df2['host'].astype(str)
            df2['installation_id'] = df2['installation_id'].astype(str)
            df2 = df2.set_index(pd.to_datetime(np.arange(len(df2)),
                                    unit='s',
                                    origin=df2.time[0]))
                    
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                    port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df2, "total_orders_created", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=None, dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        print("data inserted")

        return None


with DAG(
    'total_orders_created',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'orders information',
    schedule_interval = '41 6 * * *',
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
            if tenant['Active'] == "Y" and tenant['total_orders_created'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                total_orders_created_final_task = PythonOperator(
                    task_id='TotalOrdersCreated{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(TotalOrdersCreated().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":"dev_postgres", "influx_ip":"dev_influxdb", "influx_port":8086,\
        #           "write_influx_ip":"dev_influxdb","write_influx_port":8086, \
        #           "out_db_name":"airflow", "alteryx_out_db_name": "airflow"}
        tenant = CommonFunction().get_tenant_info()
        total_orders_created_final_task = PythonOperator(
            task_id='total_orders_created_final',
            provide_context=True,
            python_callable=functools.partial(TotalOrdersCreated().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )
