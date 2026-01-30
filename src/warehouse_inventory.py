## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, Butler_api
from pandasql import sqldf
import time
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')



class warehouse_inventory:

    def get_dtype_of_field(self, df, field_name):
        filed_type = 'string'
        if not df.empty:
            filed_type = df[df['fieldKey'] == field_name]
            if not filed_type.empty:
                filed_type = filed_type.reset_index()
                filed_type = filed_type['fieldType'][0]
        if filed_type == 'float':
            return float
        elif filed_type == 'integer':
            return int
        else:
            return str


    def warehouse_inventory(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.utilfunction = CommonFunction()
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.influxclient = self.client.influx_client
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        self.butler = Butler_api(host=self.tenant_info['Butler_ip'], port=self.tenant_info['Butler_port'])
        df2_1st=self.butler.fetch_inventoryinfo_data()
        if not df2_1st.empty:
            #self.read_client1 = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"], db="Alteryx")
            #self.read_influxclient1 = self.read_client1.influx_client
            q = f"select cron_ran_at from rack_volume_utlization where time >now()-7d order by time desc limit 1"
            cron_ran_at = self.client.fetch_data(self.influxclient, q)
            if 'error' in cron_ran_at[0].keys():
                print("error in query")
            else:
                cron_ran_at_date = cron_ran_at[0]["cron_ran_at"]
                q = f"select * from rack_volume_utlization where time >= now()-7d and cron_ran_at = '{cron_ran_at_date}'"
                print(q)
                df = pd.DataFrame(self.influxclient.query(q).get_points())

                df = df[['rack_type', 'total_slots', 'shelf_floor']]
                df = df.groupby(['rack_type', 'shelf_floor'], as_index=False).agg(total_slots=('total_slots', 'sum'))
                df.rename(columns={'shelf_floor': 'floor'}, inplace=True)
                df2_1st.rename(columns={'total_slots': 'used_slots'}, inplace=True)
                final_df = pd.merge(df, df2_1st, on=['rack_type', 'floor'], how='left')
                cron_ran_at = datetime.now()
                cron_ran_at = cron_ran_at.replace(microsecond=0)
                final_df['cron_ran_at'] = cron_ran_at
                final_df = self.utilfunction.reset_index(final_df)
                final_df = self.utilfunction.get_epoch_time(final_df, date1=cron_ran_at)
                final_df['time'] = pd.to_datetime(final_df.time_ns)

                final_df = final_df.drop(columns=['time_ns', 'index'])
                l = ['time_ns', 'index', 'level_0']
                for col in l:
                    if col in final_df.columns:
                        del final_df[col]
                final_df.cron_ran_at = final_df.cron_ran_at.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
                final_df = final_df[~pd.isna(final_df['unique_skus'])].reset_index(drop=True)
                final_df['unique_skus'] = final_df['unique_skus'].astype(int)
                q = "SHOW FIELD KEYS FROM  warehouse_inventory"
                df = pd.DataFrame(self.client.query(q).get_points())
                sku_quanity_type = self.get_dtype_of_field(df, 'sku_quanity')
                final_df['sku_quanity'] = final_df['sku_quanity'].astype(sku_quanity_type)
                final_df = final_df.set_index('time')
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"])
                self.write_client.writepoints(final_df, "warehouse_inventory", db_name=self.tenant_info["alteryx_out_db_name"],
                                              tag_columns=['floor','face','rack_type'], dag_name=os.path.basename(__file__),
                                              site_name=self.tenant_info['Name'])


        return None
#
# # -----------------------------------------------------------------------------
# ## Task definations
# ## -----------------------------------------------------------------------------
# with DAG(
#     'warehouse_inventory',
#     default_args = CommonFunction().get_default_args_for_dag(),
#     description = 'calculation of warehouse_inventory',
#     schedule_interval=timedelta(hours=8),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# ) as dag:
#     import csv
#     import os
#     import functools
#     if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
#         csvReader = CommonFunction().get_all_site_data_config()
#         for tenant in csvReader:
#             if tenant['Active'] == "Y" and tenant['warehouse_inventory'] == "Y":
#                 warehouse_inventory_final_task = PythonOperator(
#                     task_id='warehouse_inventory_final_{}'.format(tenant['Name']),
#                     provide_context=True,
#                     python_callable=functools.partial(warehouse_inventory().warehouse_inventory,tenant_info={'tenant_info': tenant}),
#                     execution_timeout=timedelta(seconds=60),
#                 )
#     else:
#         # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
#         #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
#         #           "out_db_name":db_name}
#         tenant = CommonFunction().get_tenant_info()
#         warehouse_inventory_final_task = PythonOperator(
#             task_id='warehouse_inventory_final',
#             provide_context=True,
#             python_callable=functools.partial(warehouse_inventory().warehouse_inventory,tenant_info={'tenant_info': tenant}),
#             op_kwargs={
#                 'tenant_info1': tenant,
#             },
#             execution_timeout=timedelta(seconds=3600),
#         )

