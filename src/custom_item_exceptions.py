## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
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
class custom_item_exceptions:
    def custom_item_exceptions(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("item_exceptions_ecom_retail", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.custom_item_exceptions1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("item_exceptions_ecom_retail", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.custom_item_exceptions1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
    def custom_item_exceptions1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("item_exceptions_ecom_retail", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        self.utilfunction = CommonFunction()
        q=f"select order_id, installation_id,order_flow_name,uom_requested from item_picked  where  time >= '{self.start_date}' and time < '{self.end_date}' order by time desc"
        order_df=pd.DataFrame(self.read_client.query(q).get_points())
        order_df = self.utilfunction.flow_event(df=order_df,site_name=self.tenant_info["Name"])
        q=f"select * from item_exceptions  where  time >= '{self.start_date}' and time < '{self.end_date}' and mode ='pick' order by time desc"
        item_exceptions_df=pd.DataFrame(self.read_client.query(q).get_points())

        if not item_exceptions_df.empty and not order_df.empty:
            df_final = pd.merge(item_exceptions_df, order_df[['installation_id', 'order_id','order_flow']], on=['installation_id', 'order_id'], how='left')
            df_final["quantity"] = df_final["quantity"].astype(float)
            df_final.time = pd.to_datetime(df_final.time)
            df_final = df_final.set_index('time')
            #df_final=df_final.drop(columns=['order_flow_name','uom_requested'])
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_final, "item_exceptions_ecom_retail", db_name=self.tenant_info["out_db_name"],
                                          tag_columns=['host', 'installation_id', 'pps_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        else:
            filter = {"site": self.tenant_info['Name'], "table": "item_exceptions_ecom_retail"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)

        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'custom_item_exceptions',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of Orderline transactions',
    schedule_interval = '48 * * * *',
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
            if tenant['Active'] == "Y" and tenant['custom_item_exceptions'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                custom_item_exceptions_final_task = PythonOperator(
                    task_id='custom_item_exceptions_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(custom_item_exceptions().custom_item_exceptions,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )

    else:
        # tenant = {"Name":"project", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        custom_item_exceptions_final_task = PythonOperator(
            task_id='custom_item_exceptions_final',
            provide_context=True,
            #python_callable=ButlerUptime().butler_uptime_final,
            python_callable=functools.partial(custom_item_exceptions().custom_item_exceptions,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

