## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')



## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class light_curtain_events:
    def get_pps_id(self, df):
        df['pps_id'] = df['event_owner'].str.split(',', expand=True)[1]
        df['pps_id'] = df['pps_id'].apply(lambda x: x.replace('}', ''))
        return df
    
    def type_cast_cols(self, df):
        df['pps_id'] = df['pps_id'].apply(lambda x: int(x))
        df['pps_id'] = df['pps_id'].astype(int)
        df['time_difference'] = df['time_difference'].astype(int)
        df['mode'] = df['mode'].astype(str)
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')
        return df

    def clean_raw_data(self, df):
        df = df.reset_index(drop=True)
        df['prev_breach_action_triggered'] = df['breach_action_triggered'].shift(1)
        df['prev_pps_id'] = df['pps_id'].shift(1)
        df['prev_installation_id'] = df['installation_id'].shift(1)
        df = df[(df['breach_action_triggered']!=df['prev_breach_action_triggered']) | (df['pps_id']!=df['prev_pps_id'])| (df['installation_id']!=df['prev_installation_id'])].reset_index(drop=True)
        if not df.empty:
            df['next_time'] = df['time'].shift(-1)
            df['next_pps_id'] = df['pps_id'].shift(-1)
            df['time_difference'] = df.apply(lambda x: (x['next_time']-x['time']).total_seconds() if ((x['event_type'] == 'light_curtain_breached') & (not pd.isna(x['next_time'])) &(x['pps_id']==x['next_pps_id'])) else x['time_difference'], axis= 1).astype(int)
            df = df[df['event_type']=='light_curtain_breached'].reset_index(drop=True)
            cols = ['time','event_type','installation_id','pps_id','time_difference']
            df = df[cols]
            return df

    def light_curtain_events_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["out_db_name"])
        #        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")
        self.utilfunction = CommonFunction()
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("light_curtain_events_airflow", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from light_curtain_events where time>'{check_start_date}' and event_type ='light_curtain_breached'  and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.light_curtain_events_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("light_curtain_events_airflow", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("light_curtain_events_airflow", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.light_curtain_events_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
    def light_curtain_events_final1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("light_curtain_events_airflow", self.tenant_info)
        self.start_date = pd.to_datetime(self.start_date) - timedelta(hours=1)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')


        q = f"select event_type from light_curtain_events where event_type='light_curtain_breached' and time>'{self.start_date}' and time <= '{self.end_date}' and event_owner =~ /pps/ order by time limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "light_curtain_events_airflow"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)
            return

        q = f"select event_owner,event_type,installation_id,breach_action_triggered from light_curtain_events where time>'{self.start_date}' and time <= '{self.end_date}' and event_owner =~ /pps/ order by time desc"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        q = f"select pps_id, mode,installation_id from pps_data where time>'{self.start_date}' and time <= '{self.end_date}' order by time desc"
        pps_mode = pd.DataFrame(self.read_client.query(q).get_points())
    
        if not df.empty:
            df = self.get_pps_id(df)
            df.time = pd.to_datetime(df.time)
            df = df.sort_values(by=['pps_id', 'time'], ascending=[True, True])
            df['time_difference'] = 0
            df = self.clean_raw_data(df)
            pps_mode = pps_mode.groupby(['installation_id','pps_id'], as_index=False)['mode'].last().reset_index()
            pps_mode['pps_id'] = pps_mode['pps_id'].astype(str, errors = 'ignore')
            df = pd.merge(df,pps_mode, how = 'left',on =['installation_id','pps_id'])
            df = self.type_cast_cols(df)
            if 'index' in df.columns:
                del df['index']
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df, "light_curtain_events_airflow", db_name=self.tenant_info["out_db_name"],
                                          tag_columns=['pps_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None

with DAG(
        'light_curtain_events',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of light_curtain_events',
        schedule_interval='*/15 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(minutes=60)
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['light_curtain_events'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                final_task = PythonOperator(
                    task_id='light_curtain_events_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(light_curtain_events().light_curtain_events_final,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='light_curtain_events',
            provide_context=True,
            python_callable=functools.partial(light_curtain_events().light_curtain_events_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

