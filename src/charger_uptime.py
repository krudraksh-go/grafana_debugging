## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient
import numpy as np
import pandas as pd

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
class ChargerUptime:
    def get_1hr_count(self, df):
        df=df.set_index('time').groupby('charger_id').resample('1H').sum()
        df=df.reset_index()
        if 'index' in df.columns:
            del df['index']
        return df

    def check_if_empty(self, df):
        if df.empty:
            df = pd.DataFrame({'charger_id': ['0'],
                               'key': [None],
                               'time': [self.end_date]}) #find from dag
            df['time'] = pd.to_datetime(df['time'], utc=True)
        else :
            df = self.get_1hr_count(df)
        return df

    def merge_dataframes(self, df1, df2, df3, df4):
        df_merged=pd.merge(df1,df2,on=['charger_id','time'],how='outer')
        df_merged2=pd.merge(df_merged,df3,on=['charger_id','time'],how='outer')
        df_merged_final=pd.merge(df_merged2,df4,on=['charger_id','time'],how='outer')
        return df_merged_final

    def charger_uptime_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        #self.client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["write_influx_port"],db='Alteryx')
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("charger_uptime_ayx", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from charger_uptime where time>'{check_start_date}' and time<='{check_end_date}' and charger_id<>'-1' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.charger_uptime_final1(self.end_date, **kwargs)
        else:
            daterange = self.client.get_datetime_interval3("charger_uptime_ayx", '8h', self.tenant_info)
            try:
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.charger_uptime_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
    
    def charger_uptime_final1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("charger_uptime_ayx", self.tenant_info)

        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        #self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q=f"select charger_id,connectivity,state from charger_uptime where  time >= '{self.start_date}' and time < '{self.end_date}' and charger_id<>'-1'  and Butler_ip ='{self.tenant_info['Butler_ip']}'"

        df_main=pd.DataFrame(self.read_client.query(q).get_points())
        if df_main.empty:
            q = f"select charger_id,connectivity,state from charger_uptime where  time >= '{self.start_date}' and time < '{self.end_date}' and charger_id<>'-1' "
            df_main = pd.DataFrame(self.read_client.query(q).get_points())

        if not df_main.empty:
            df_main['time']=pd.to_datetime(df_main['time'])
            df_main['key']=1
            df_online = df_main.loc[(df_main['state'] == 'auto') & (df_main['connectivity'] == 'connected')]
            df_offline = df_main.loc[(df_main['connectivity'] == 'disconnected')]
            df_error = df_main.loc[(df_main['state'] != 'auto' ) & (df_main['connectivity'] == 'connected')]
            df_main = self.check_if_empty(df_main)
            df_online = self.check_if_empty(df_online)
            df_offline = self.check_if_empty(df_offline)
            df_error = self.check_if_empty(df_error)
            df_main.rename(columns = {'key':'total_time'}, inplace = True)
            df_online.rename(columns = {'key':'online_time'}, inplace = True)
            df_offline.rename(columns = {'key':'offline_time'}, inplace = True)
            df_error.rename(columns = {'key':'error_time'}, inplace = True)
            df_final = self.merge_dataframes(df_main,df_online,df_offline,df_error)
            df_final = df_final[(pd.isna(df_final["charger_id"]) == False)]
            df_final['flag'] = np.where((df_final['offline_time'].notna() & (df_final['offline_time']/df_final['total_time'] >= 0.8)), 'N','Y')
            if 'charger_id_int' not in df_final.columns:
                df_final['charger_id'] = df_final['charger_id'].astype(str)
                df_final['charger_id_int'] = df_final['charger_id'].astype(float)
            df_final = df_final.fillna(0)
            df_final['total_time'] = df_final['total_time'].astype(float)
            df_final['error_time'] = df_final['error_time'].astype(float)
            df_final['offline_time'] = df_final['offline_time'].astype(float)
            df_final['online_time'] = df_final['online_time'].astype(float)
            df_final['flag'] = df_final['flag'].astype(str)
            df_final['host']= self.tenant_info['host_name']
            df_final['installation_id'] = self.tenant_info['installation_id']
            df_final['host'] = df_final['host'].astype(str)
            df_final['installation_id'] = df_final['installation_id'].astype(str)

            df_final.time = pd.to_datetime(df_final.time)
            df_final = df_final.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_final, "charger_uptime_ayx", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=['charger_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        else:
            filter = {"site": self.tenant_info['Name'], "table": "charger_uptime_ayx"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'Charger_uptime',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of charger uptime',
    schedule_interval = "5 7,15,23 * * *",
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
            if tenant['Active'] == "Y" and tenant['Charger_uptime'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                charger_uptime_final_task = PythonOperator(
                    task_id='charger_uptime_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(ChargerUptime().charger_uptime_final,tenant_info={'tenant_info': tenant}),
                )
                execution_timeout = timedelta(seconds=600),
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        charger_uptime_final_task = PythonOperator(
            task_id='charger_uptime_final',
            provide_context=True,
            #python_callable=ChargerUptime().charger_uptime_final,
            python_callable=functools.partial(ChargerUptime().charger_uptime_final,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )


