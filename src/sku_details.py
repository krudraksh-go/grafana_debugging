## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
import time
import json
from config import (
    MongoDbServer
)

from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, postgres_connection,Butler_api, MongoDBManager
from pandasql import sqldf
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
#     'orderline_transactions',
#     default_args = default_args,
#     description = 'calculation of orderline_transactions GM-44036',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class cls_sku_details:

    def insert_data_to_mongo_db(self, df):
        try:
            df = df.reset_index()
            df['time'] = df['time'].apply(lambda x: self.utilfunction.convert_to_nanosec(x))
            connection_string = MongoDbServer
            collection_name = self.tenant_info['Name'] + '_sku_details'
            database_name = "GreyOrange"
            self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
            json_list = df.to_json(orient="records")
            json_list = json.loads(json_list)
            test = self.cls_mdb.insert_data(json_list)
            print("Data Inserted to MongoDB!!")
        except Exception as ex:
            print(f"Unable to Insert Data to MongoDB Exception: {ex}")
        
    def def_sku_details(self, tenant_info, **kwargs):
        Today = time.strftime('%Y-%m-%d')
        Kal = datetime.today() - timedelta(days=1)
        Parso = datetime.today() - timedelta(days=2)
        Yesterday = Kal.strftime('%Y-%m-%d')
        dby = Parso.strftime('%Y-%m-%d')

        self.tenant_info = tenant_info['tenant_info']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.influxclient = self.client.influx_client
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        self.utilfunction = CommonFunction()
        q = f"select max(item_id) as item_id from sku_details"
        sku_details_df = pd.DataFrame(self.client.query(q).get_points())
        if sku_details_df.empty:
            sku_details_last_id = 0
        else:
            sku_details_last_id = sku_details_df['item_id'][0]

        if self.tenant_info['Postgres_pf_user'] != "":
            self.postgres_conn = postgres_connection(database='wms_masterdata',
                                                     user=self.tenant_info['Postgres_pf_user'], \
                                                     sslrootcert=self.tenant_info['sslrootcert'],
                                                     sslcert=self.tenant_info['sslcert'], \
                                                     sslkey=self.tenant_info['sslkey'],
                                                     host=self.tenant_info['Postgres_pf'], \
                                                     port=self.tenant_info['Postgres_pf_port'],
                                                     password=self.tenant_info['Postgres_pf_password'],
                                                     dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])


            self.postgres_conn.pg_curr.execute("select count(*) from item where id>"+str(sku_details_last_id))
            totalcount = self.postgres_conn.pg_curr.fetchall()
            self.postgres_conn.close()
            print(f"total count{totalcount}")
            if totalcount[0][0]==0:
                return None

            f = True
            time_offset = 0
            l = 75000
            dfs = []
            tday = datetime.today()
            print(tday)
            while f == True:
                if time_offset <= totalcount[0][0]:
                    self.postgres_conn = postgres_connection(database='wms_masterdata',
                                                             user=self.tenant_info['Postgres_pf_user'], \
                                                             sslrootcert=self.tenant_info['sslrootcert'],
                                                             sslcert=self.tenant_info['sslcert'], \
                                                             sslkey=self.tenant_info['sslkey'],
                                                             host=self.tenant_info['Postgres_pf'], \
                                                             port=self.tenant_info['Postgres_pf_port'],
                                                             password=self.tenant_info['Postgres_pf_password'],
                                                             dag_name=os.path.basename(__file__),
                                                             site_name=self.tenant_info['Name'])

                    self.postgres_conn.pg_curr.execute(
                        "select id, (productattributes ->> 'product_sku') as sku , (productattributes ->> 'brand') as brand, \
                        (productattributes ->> 'max_quantity') as max_quantity , (productattributes ->> 'min_quantity') as min_quantity, \
                        (productattributes ->> 'max_days_in_hand') as max_DOH, (productattributes ->> 'min_days_in_hand') as min_DOH \
                         from item where id>"+str(sku_details_last_id)+ " order by id offset " + str(
                            time_offset) + " limit " + str(l) + "")
                    batch = self.postgres_conn.pg_curr.fetchall()

                    df = pd.DataFrame(batch, columns=['item_id', 'sku_id', 'brand','max_quantity', 'min_quantity', 'max_DOH', 'min_DOH'])
                    possible_values = ['item_id', 'sku_id', 'brand', 'max_quantity', 'min_quantity', 'max_DOH', 'min_DOH']
                    for val in possible_values:
                        if val not in df.columns:
                            df[val] = 0

                    # posting batch to influx
                    dte = str(tday).split(' ')[0]
                    df['date'] = dte
                    df['brand'].fillna("NA", inplace=True)
                    if not df.empty:
                    #    df = df.set_index(pd.to_datetime(np.arange(len(df)), unit='ns', origin=df['date'][0]))
                        df = self.utilfunction.get_epoch_time(df, date1=tday)
                        df['time'] = pd.to_datetime(df.time_ns)
                        df = df.drop(columns=['time_ns'])
                        df['host'] = self.tenant_info['host_name']
                        df['installation_id'] = self.tenant_info['installation_id']
                        df['host'] = df['host'].astype(str)
                        df['installation_id'] = df['installation_id'].astype(str)
                        df['sku_id'] = df['sku_id'].astype(str).str.replace('"', '', regex=False)
                        df['sku_id'] = df['sku_id'].astype(str)
                        df = df.set_index('time')
                        if 'index' in df.columns:
                            df = df.drop(columns=['index'])

                        # client = DataFrameClient(host=str(influxip), port=str(influxport), username='gor',
                        #                          password='Grey()orange',
                        #                          database='Alteryx')
                        # client.write_points(df, 'sku_details', database='Alteryx', protocol='line', time_precision='s')
                        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                            port=self.tenant_info["write_influx_port"])
                        self.write_client.writepoints(df[['item_id', 'sku_id', 'brand','date']], "sku_details",
                                                    db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                        self.insert_data_to_mongo_db(df)
                    print('Data inserted')

                    time_offset = time_offset + 75000
                    tday = tday + timedelta(hours=1)
                    self.postgres_conn.close()
                else:
                    f = False
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'sku_details',
    default_args = {**CommonFunction().get_default_args_for_dag(), **{'retries':3, 'retry_delay':timedelta(minutes=1)}},
    description = 'calculation of sku_details',
    schedule_interval =  '47 1 * * *',
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
            if tenant['Active'] == "Y"  and tenant['sku_details'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                sku_details_final_task = PythonOperator(
                    task_id='sku_details_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(cls_sku_details().def_sku_details,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )

    else:
        # tenant = {"Name":"project", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        sku_details_final_task = PythonOperator(
            task_id='sku_details_final',
            provide_context=True,
            #python_callable=ButlerUptime().butler_uptime_final,
            python_callable=functools.partial(cls_sku_details().def_sku_details,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

