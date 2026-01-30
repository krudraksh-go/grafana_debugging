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
class custom_pps_operator_log_with_sector:
    def custom_pps_operator_log_with_sector(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("pps_operator_log_sector", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from pps_data where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.custom_pps_operator_log_with_sector1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("pps_operator_log_sector", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.custom_pps_operator_log_with_sector1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def custom_pps_operator_log_with_sector1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("pps_operator_log_sector", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        self.utilfunction = CommonFunction()

        q="SHOW FIELD KEYS FROM  pps_operator_log_sector "
        df = pd.DataFrame(self.client.query(q).get_points())
        value_type= "float"
        if not df.empty:
            value_type= df[df['fieldKey']=='value']
            if not value_type.empty:
                value_type=value_type.reset_index()
                if value_type.shape[0]>1:
                    value_type=""
                else:
                    value_type=value_type['fieldType'][0]
            else:
                value_type = "float"

        q=f"select installation_id,pps_id, profile, mtu_type_id, order_flow from pps_data  where  time >= '{self.start_date}' and time < '{self.end_date}' order by time desc"
        order_df=pd.DataFrame(self.read_client.query(q).get_points())
        order_df = self.utilfunction.flow_event_for_profile(order_df,site_name=self.tenant_info["Name"])
        if not order_df.empty:
            order_df['time2'] =order_df.time.apply(lambda x:pd.to_datetime(x).replace(second=0, microsecond=0))
        q=f"select * from pps_operator_log  where  time >= '{self.start_date}' and time < '{self.end_date}' and pps_mode ='pick_mode' and value> 3 and value< 550 order by time desc"
        pps_operator_log_df=pd.DataFrame(self.read_client.query(q).get_points())

        if not pps_operator_log_df.empty:
            pps_operator_log_df['time2'] = pps_operator_log_df.time.apply(lambda x: pd.to_datetime(x) + timedelta(minutes=5 - pd.to_datetime(x).minute % 5))
            pps_operator_log_df['time2'] = pps_operator_log_df.time2.apply(lambda x: x.replace(second=0, microsecond=0))
            df_final = pd.merge(pps_operator_log_df, order_df[['installation_id', 'pps_id','time2','order_flow']], on=['installation_id', 'pps_id','time2'])
            df_final.time = pd.to_datetime(df_final.time)
            df_final = df_final.set_index('time')
            df_final=df_final.drop(columns=['time2'])

            if value_type =='float':
                df_final["value"] = df_final['value'].astype(float)
            elif value_type =='string':
                df_final["value"] = df_final['value'].astype(str)
            else:
                df_final = df_final.drop(columns=['value'])

            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_final, "pps_operator_log_sector", db_name=self.tenant_info["out_db_name"],
                                          tag_columns=['butler_id','host', 'installation_id', 'pps_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            print("inserted")
        else:
            filter = {"site": self.tenant_info['Name'], "table": "pps_operator_log_sector"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'custom_pps_operator_log_with_sector',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of Orderline transactions',
    schedule_interval = '42 * * * *',
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
            if tenant['Active'] == "Y" and tenant['custom_pps_operator_log_with_sector'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                custom_pps_operator_log_with_sector_final_task = PythonOperator(
                    task_id='custom_pps_operator_log_with_sector_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(custom_pps_operator_log_with_sector().custom_pps_operator_log_with_sector,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"Adidas", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        custom_pps_operator_log_with_sector_final_task = PythonOperator(
            task_id='custom_pps_operator_log_with_sector_final',
            provide_context=True,
            #python_callable=ButlerUptime().butler_uptime_final,
            python_callable=functools.partial(custom_pps_operator_log_with_sector().custom_pps_operator_log_with_sector,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

