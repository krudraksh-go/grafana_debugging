
## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
import numpy as np
import pandas as pd

## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

# import os
# Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
# influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
# influx_port = os.environ.get('INFLUX_PORT', '8086')
# write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
# db_name =os.environ.get('Out_db_name', 'airflow')
#end_date=None

#
# dag = DAG(
#     'butler_uptime',
#     default_args = default_args,
#     description = 'calculation of butler uptime',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )
#
## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class ButlerUptime:
    def drop_columns(self, df):
        df=df.reset_index().drop(columns=['low_charged_paused','manual_paused','bot_state','power_state','tasktype'])
        return df

    def check_if_empty(self, df):
        if df.empty:
            df = pd.DataFrame({'bot_id': [None],'category':[None],
                               'key': 0,
                               'time': ['2022-08-13 06:00:00']}) #get end date from DAG
            df['time'] = df['time'].apply(pd.to_datetime, utc=True)
        else :
            df = self.get_1hr_count(df)
        return df

    def get_1hr_count(self, df):
        df=df.set_index('time').groupby(['bot_id','category']).resample('1H').sum()
        df=df.reset_index()
        if 'index' in df.columns:
            del df['index']
        return df

    def merge_dataframes(self, df1,df2,df3,df4,df5,df6,df7,df8,df9,df10):
        df_merged=pd.merge(df1,df2,on=['bot_id','category','time'],how='outer')
        df_merged2=pd.merge(df_merged,df3,on=['bot_id','category','time'],how='outer')
        df_merged3=pd.merge(df_merged2,df4,on=['bot_id','category','time'],how='outer')
        df_merged4=pd.merge(df_merged3,df5,on=['bot_id','category','time'],how='outer')
        df_merged5=pd.merge(df_merged4,df6,on=['bot_id','category','time'],how='outer')
        df_merged6=pd.merge(df_merged5,df7,on=['bot_id','category','time'],how='outer')
        df_merged7=pd.merge(df_merged6,df8,on=['bot_id','category','time'],how='outer')
        df_merged8=pd.merge(df_merged7,df9,on=['bot_id','category','time'],how='outer')
        df_merged_final=pd.merge(df_merged8,df10,on=['bot_id','category','time'],how='outer')
        return df_merged_final

    def butler_uptime_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
        self.site = self.tenant_info['Name']

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("butler_uptime_ayx", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")


        q=f"select * from butler_uptime  where  time >= '{check_start_date}' and time < '{check_end_date}' and bot_id <> '-1' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())

        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.butler_uptime_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("butler_uptime_ayx", '8h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.butler_uptime_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def butler_uptime_final1(self, end_date, **kwargs):

        self.start_date = self.client.get_start_date("butler_uptime_ayx", self.tenant_info)
        self.utilfunction  = CommonFunction()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q=f"select bot_id,low_charged_paused,manual_paused,bot_state,power_state,tasktype,category from butler_uptime  where  time >= '{self.start_date}' and time < '{self.end_date}' and bot_id <> '-1' and Butler_ip ='{self.tenant_info['Butler_ip']}'"
        df=pd.DataFrame(self.read_client.query(q).get_points())

        if df.empty:
            q = f"select bot_id,low_charged_paused,manual_paused,bot_state,power_state,tasktype from butler_uptime  where  time >= '{self.start_date}' and time < '{self.end_date}' and bot_id <> '-1'"
            df = pd.DataFrame(self.read_client.query(q).get_points())

        if 'category' not in df.columns:
            df['category']=''

        if not df.empty:
            df['time']=pd.to_datetime(df['time'])
            df['key']=1
            df_total = df
            df_idle=df.loc[(df['low_charged_paused'] == False) & (df['manual_paused'] == False) & ((df['bot_state'] == 'processing') | (df['bot_state'] == 'ready') | (df['bot_state'] == 'assigned')) & (df['power_state'] == 'online') & (df['tasktype'] == 'null')]
            df_error=df.loc[((df['bot_state'] == 'initializing') & (df['power_state'] == 'online')) | ((df['bot_state'] != 'dead') & (df['power_state'] == 'error')) | (df['bot_state'] == 'error')]
            df_offline=df.loc[(df['bot_state'] == 'dead')]

            df_pps_task=df.loc[((df['tasktype'] == 'audittask') | (df['tasktype'] == 'reconfigtask') | (df['tasktype'] == 'picktask') | (df['tasktype'] == 'auxiliarytask') | (df['tasktype'] == 'postpicktask')) & ((df['bot_state'] == 'processing') | (df['bot_state'] == 'assigned')) & (df['power_state'] == 'online')]
            df_charging=df.loc[(df['tasktype'] == 'chargetask') & ((df['bot_state'] == 'processing') | (df['bot_state'] == 'assigned')) & (df['power_state'] == 'online')]
            df_move_task=df.loc[(df['power_state'] == 'online') & ((df['bot_state'] == 'processing') | (df['bot_state'] == 'assigned')) & ((df['tasktype'] == 'movetask') | (df['tasktype'] == 'preputtask'))]
            df_rearrangement=df.loc[(df['tasktype'] == 'msurearrangementtask') & (df['power_state'] == 'online') & (df['bot_state'] != 'dead') & (df['bot_state'] != 'error') & (df['bot_state'] != 'initializing')]
            df_manual_paused=df.loc[(df['manual_paused'] == True) & (df['low_charged_paused'] == False) & (df['power_state'] == 'online') & (df['bot_state'] != 'dead') & (df['bot_state'] != 'error') & (df['bot_state'] != 'initializing') & (df['tasktype'] == 'null')]
            df_low_charge_paused=df.loc[(df['low_charged_paused'] == True) & (df['power_state'] == 'online') & (df['bot_state'] != 'dead') & (df['bot_state'] != 'error') & (df['bot_state'] != 'initializing') & (df['tasktype'] == 'null')]
            df_total=self.drop_columns(df_total)
            df_idle=self.drop_columns(df_idle)
            df_error=self.drop_columns(df_error)
            df_offline=self.drop_columns(df_offline)
            df_pps_task=self.drop_columns(df_pps_task)
            df_charging=self.drop_columns(df_charging)
            df_move_task=self.drop_columns(df_move_task)
            df_rearrangement=self.drop_columns(df_rearrangement)
            df_manual_paused=self.drop_columns(df_manual_paused)
            df_low_charge_paused=self.drop_columns(df_low_charge_paused)
            df_total=self.check_if_empty(df_total)
            df_idle=self.check_if_empty(df_idle)
            df_error=self.check_if_empty(df_error)
            df_offline=self.check_if_empty(df_offline)
            df_pps_task=self.check_if_empty(df_pps_task)
            df_charging=self.check_if_empty(df_charging)
            df_move_task=self.check_if_empty(df_move_task)
            df_rearrangement=self.check_if_empty(df_rearrangement)
            df_manual_paused=self.check_if_empty(df_manual_paused)
            df_low_charge_paused=self.check_if_empty(df_low_charge_paused)
            df_total.rename(columns = {'key':'total_time'}, inplace = True)
            df_idle.rename(columns = {'key':'idle_time'}, inplace = True)
            df_error.rename(columns = {'key':'error_time'}, inplace = True)
            df_offline.rename(columns = {'key':'offline_time'}, inplace = True)
            df_pps_task.rename(columns = {'key':'pps_task_time'}, inplace = True)
            df_charging.rename(columns = {'key':'charging_time'}, inplace = True)
            df_move_task.rename(columns = {'key':'move_task'}, inplace = True)
            df_rearrangement.rename(columns = {'key':'rearrangement'}, inplace = True)
            df_manual_paused.rename(columns = {'key':'manual_paused'}, inplace = True)
            df_low_charge_paused.rename(columns = {'key':'low_charged_paused'}, inplace = True)
            df_final=self.merge_dataframes(df_total,df_idle,df_error,df_offline,df_pps_task,df_charging,df_move_task,df_rearrangement,df_manual_paused,df_low_charge_paused)
            df_final['flag']=np.where((df_final['offline_time'].notna() & (df_final['offline_time']/df_final['total_time'] >= 0.8)), 'N','Y')
            df_final = df_final[(pd.isna(df_final["bot_id"]) == False)]
            df_final = df_final.fillna(0)
            df_final = df_final[(df_final["total_time"]!=0)]
            df_final['total_kpi']=df_final['idle_time']+df_final['pps_task_time']+df_final['charging_time']+df_final['offline_time']+df_final['move_task']+df_final['rearrangement']+df_final['low_charged_paused']+df_final['manual_paused']+df_final['error_time']
            df_final['total_misc']=df_final['total_time']-df_final['total_kpi']
            if 'bot_id_int' not in df_final.columns:
                df_final['bot_id']= df_final['bot_id'].astype(str)
                df_final['bot_id_int'] = df_final['bot_id'].astype(int)
            df_final['bot_id_int'] = df_final['bot_id_int'].astype(float)
            df_final['charging_time'] = df_final['charging_time'].astype(float)
            df_final['error_time'] = df_final['error_time'].astype(float)
            df_final['idle_time'] = df_final['idle_time'].astype(float)
            df_final['low_charged_paused'] = df_final['low_charged_paused'].astype(float)
            df_final['manual_paused'] = df_final['manual_paused'].astype(float)
            df_final['move_task'] = df_final['move_task'].astype(float)
            df_final['offline_time'] = df_final['offline_time'].astype(float)
            df_final['pps_task_time'] = df_final['pps_task_time'].astype(float)
            df_final['rearrangement'] = df_final['rearrangement'].astype(float)
            df_final['total_kpi'] = df_final['total_kpi'].astype(float)
            df_final['total_misc'] = df_final['total_misc'].astype(float)
            df_final['total_time'] = df_final['total_time'].astype(float)
            df_final['flag'] = df_final['flag'].astype(str)
            df_final['host']= self.tenant_info['host_name']
            df_final['installation_id'] = self.tenant_info['installation_id']
            df_final['host'] = df_final['host'].astype(str)
            df_final['installation_id'] = df_final['installation_id'].astype(str)
            df_final.time = pd.to_datetime(df_final.time)
            df_final = df_final.set_index('time')
            if 'index' in df_final.columns:
                del df_final['index']
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_final, "butler_uptime_ayx", db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['bot_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        else:
            filter = {"site": self.tenant_info['Name'], "table": "butler_uptime_ayx"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)
        return None


# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'butler_uptime',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of butler uptime',
    schedule_interval = "2 7,15,23 * * *",
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
            if tenant['Active'] == "Y" and tenant['butler_uptime'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                butler_uptime_final_task = PythonOperator(
                    task_id='butler_uptime_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(ButlerUptime().butler_uptime_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        butler_uptime_final_task = PythonOperator(
            task_id='butler_uptime_final',
            provide_context=True,
            python_callable=functools.partial(ButlerUptime().butler_uptime_final,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )



