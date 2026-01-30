import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction,postgres_connection
# from utils.CommonFunction1 import InfluxData, Write_InfluxData, CommonFunction,postgres_connection
from pandasql import sqldf
import pytz
import time
import numpy as np
import os
import psycopg2


class PathEfficiency():
    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        try:
            daterange = self.client.get_datetime_interval3("path_efficiency", '1d', self.tenant_info)
            for i in daterange.index:
                self.end_date = daterange['end_date'][i]
                self.final_call1(self.end_date, **kwargs)
            # self.end_date=datetime.now(timezone.utc) - pd.Timedelta('50h')
            # self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            # self.final_call1(self.end_date,**kwargs)
        except AirflowTaskTimeout as timeout_exception:
            raise timeout_exception             
        except Exception as e:
            print(f"error:{e}")
            raise e

    def final_call1(self, end_date, **kwargs):
        self.end_date = end_date
        self.start_date = self.client.get_start_date("path_efficiency", self.tenant_info)
        self.start_date=pd.to_datetime(self.start_date)
        self.start_date=self.start_date.strftime('%Y-%m-%d')
        # self.end_date = self.end_date.replace(second=0,microsecond=0)
        # self.start_date = self.start_date.strftime('%Y-%m-%d')
        self.end_date = self.end_date.strftime('%Y-%m-%d')
        print(f"start date: {self.start_date}, end date: {self.end_date}")
        # self.start_date = datetime.now(timezone.utc) - pd.Timedelta('51h')
        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')


        self.connection = postgres_connection(database='butler_dev',
                                                         user=self.tenant_info['Postgres_butler_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_ButlerDev'], \
                                                         port=self.tenant_info['Postgres_butler_port'],
                                                         password=self.tenant_info['Postgres_butler_password'],
                                                         dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        client_cursor = self.connection.pg_curr
        client_cursor.execute("select count(*) from path_calculation_efficiency_sql_table where created_at>='"+self.start_date+"' and created_at<'"+self.end_date+"'")
        tcount = client_cursor.fetchall()

        f=True
        time_offset = 0
        l = 20000
        c=0
        # print("before whle loop!!")
        while f==True:
            # print("after f==true condition")
            if time_offset<=tcount[0][0]:
                print("in if case")
                cursor = self.connection.pg_curr
                # print("cursor before execute")
                cursor.execute("select acceleration_steps,bot_turns,butler_id,created_at,deceleration_steps,destination,destination_rack_direction,final_destination,installation_id,max_velocity_steps,path_options,path_string,pps_id,rack_turns,reference,single_steps,source,source_rack_direction,steps,subtask_type,task_key,task_type,total_time from path_calculation_efficiency_sql_table where created_at>='"+self.start_date+"' and created_at<'"+self.end_date+"' order by created_at offset "+str(time_offset)+" limit "+str(l)+"")
                # print("after cursor.execute")
                df = cursor.fetchall()
                df = pd.DataFrame(df,columns=['acceleration_steps','bot_turns','butler_id','created_at','deceleration_steps','destination','destination_rack_direction','final_destination','installation_id','max_velocity_steps','path_options','path_string','pps_id','rack_turns','reference','single_steps','source','source_rack_direction','steps','subtask_type','task_key','task_type','total_time'])
                df['destination_x'] = df['destination'].apply(lambda x:int(x.split(',')[0].replace('{','')))
                df['destination_y'] = df['destination'].apply(lambda x:int(x.split(',')[1].replace('}','')))
                df['f_destination_x'] = df['final_destination'].apply(lambda x:int(x.split(',')[0].replace('{','')))
                df['f_destination_y'] = df['final_destination'].apply(lambda x:int(x.split(',')[1].replace('}','')))
                df['source_x'] = df['source'].apply(lambda x:int(x.split(',')[0].replace('{','')))
                df['source_y'] = df['source'].apply(lambda x:int(x.split(',')[1].replace('}','')))
                df['steps_source_fd'] = (abs(df['source_x'] - df['f_destination_x'])) + (abs(df['source_y'] - df['f_destination_y']))
                df['steps_source_d'] = (abs(df['source_x'] - df['destination_x'])) + (abs(df['source_y'] - df['destination_y']))
                print(f"df before drop is {df}")
                df.drop(['source_x','source_y','destination_x','destination_y','f_destination_x','f_destination_y'],axis=1,inplace=True)
                
                #posting batch to influx
                df['created_at'] = df['created_at'].astype('str')
                df['created_at'] = df['created_at'].apply(lambda x:x[:-6])
                df['created_at'] = df['created_at'].astype('datetime64[ns]')
                df['host'] = self.tenant_info['host_name']
                df['installation_id'] = self.tenant_info['installation_id']
                df['host'] = df['host'].astype(str)
                df['installation_id'] = df['installation_id'].astype(str)

                df = df.set_index('created_at')
                # print(f" df is {df}")
                # client = DataFrameClient(host='172.19.40.56', port='8086', username='gor', password='Grey()orange')
                # client.write_points(df,'path_efficiency',database='Alteryx',protocol='line',time_precision='n')
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"], )
                self.write_client.writepoints(df, "path_efficiency", db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=None, dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        
                time.sleep(5)
                print('Data inserted',c+1)

                time_offset = time_offset+l
                c=c+1
            else:
                f=False 

        return None

with DAG(
    'path_efficiency',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'path efficiency',
    schedule_interval = '45 6 * * *',
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
            if tenant['Active'] == "Y" and tenant['path_efficiency'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                path_efficiency_final_task = PythonOperator(
                    task_id='path_efficiency_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(PathEfficiency().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":"dev_postgres", "influx_ip":"dev_influxdb", "influx_port":8086,\
        #           "write_influx_ip":"dev_influxdb","write_influx_port":8086, \
        #           "out_db_name":"airflow", "alteryx_out_db_name": "airflow"}
        tenant = CommonFunction().get_tenant_info()
        path_efficiency_final_task = PythonOperator(
            task_id='path_efficiency_final',
            provide_context=True,
            python_callable=functools.partial(PathEfficiency().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )