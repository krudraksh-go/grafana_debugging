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
class custom_pps_operator_log:
    def custom_pps_operator_log(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("pps_operator_log_ecom_retail", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")
        q = f"select * from pps_data  where  time >= '{check_start_date}' and time < '{check_end_date}' limit 1"
        if 'PROJECT' in self.tenant_info["Name"].upper() or 'HNMCANADA' in self.tenant_info["Name"].upper():
            q=f"select * from mtu_data  where  time >= '{check_start_date}' and time < '{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.custom_pps_operator_log1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("pps_operator_log_ecom_retail", '30min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.custom_pps_operator_log1(self.end_date, **kwargs)
                    # break
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def custom_pps_operator_log1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("pps_operator_log_ecom_retail", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        print(self.start_date, self.end_date)

        self.utilfunction = CommonFunction()

        if 'PROJECT' in self.tenant_info["Name"].upper() or 'HNMCANADA' in self.tenant_info["Name"].upper():
            q=f"select installation_id,pps_id, mtu_type_id ,profile from mtu_data  where  time >= '{self.start_date}' and time < '{self.end_date}' order by time desc"
        else:
            q = f"select installation_id,pps_id, profile, mtu_type_id from pps_data  where  time >= '{self.start_date}' and time < '{self.end_date}' order by time desc"

        order_df=pd.DataFrame(self.read_client.query(q).get_points())
        order_df = self.utilfunction.flow_event_for_profile(order_df,site_name=self.tenant_info["Name"])
        if not order_df.empty:
            order_df['time2'] =order_df.time.apply(lambda x:pd.to_datetime(x).replace(second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ'))
            order_df = order_df.drop_duplicates(subset=['installation_id','pps_id','time2'], keep='first')
        q=f"select * from pps_operator_log  where  time >= '{self.start_date}' and time < '{self.end_date}' and pps_mode ='pick_mode' and value> 3 and value< 550 order by time desc"
        pps_operator_log_df=pd.DataFrame(self.read_client.query(q).get_points())

        if not pps_operator_log_df.empty and not order_df.empty:
            pps_operator_log_df['time2'] = pps_operator_log_df.time.apply(lambda x: pd.to_datetime(x) + timedelta(minutes=5 - pd.to_datetime(x).minute % 5))
            pps_operator_log_df['time2'] = pps_operator_log_df.time2.apply(lambda x: x.replace(second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ'))
            df_final = pd.merge(pps_operator_log_df, order_df[['installation_id', 'pps_id','time2','order_flow']], on=['installation_id', 'pps_id','time2'], how = 'left')
            df_final = df_final.sort_values(['installation_id', 'pps_id', 'time2']).reset_index(drop=True)
            for index, row in df_final.iterrows():
                if pd.isna(row['order_flow']) and row['pps_id'] == df_final.iloc[index-1]['pps_id']:
                    df_final.at[index,'order_flow'] = df_final.iloc[index-1]['order_flow']     
            df_final.time = pd.to_datetime(df_final.time)
            df_final['order_flow'] = df_final['order_flow'].fillna('')
            df_final = df_final.set_index('time')
            df_final=df_final.drop(columns=['time2'])
            df_final['value'] = df_final['value'].astype(float, errors='ignore')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_final, "pps_operator_log_ecom_retail", db_name=self.tenant_info["out_db_name"],
                                          tag_columns=['butler_id','host', 'installation_id', 'pps_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        else:
            filter = {"site": self.tenant_info['Name'], "table": "pps_operator_log_ecom_retail"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'custom_pps_operator_log',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of Orderline transactions',
    schedule_interval = '*/30 * * * *',
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
            if tenant['Active'] == "Y" and tenant['custom_pps_operator_log'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                custom_pps_operator_log_final_task = PythonOperator(
                    task_id='custom_pps_operator_log_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(custom_pps_operator_log().custom_pps_operator_log,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"project", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        custom_pps_operator_log_final_task = PythonOperator(
            task_id='custom_pps_operator_log_final',
            provide_context=True,
            #python_callable=ButlerUptime().butler_uptime_final,
            python_callable=functools.partial(custom_pps_operator_log().custom_pps_operator_log,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

