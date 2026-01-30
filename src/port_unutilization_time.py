## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import numpy as np
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

class port_unutilization_time:
    def port_unutilization_time_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        #        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("port_unutilization_time", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from port_load_unload_events where time>'{check_start_date}' and time<='{check_end_date}'  limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.port_unutilization_time_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("port_unutilization_time", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("port_unutilization_time", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.port_unutilization_time_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def calculation_time(self,df,df3):
        if not df.empty:
            if not df3.empty:
                df= pd.concat([df,df3])
            df = df.sort_values(by=['installation_id','port_id', 'time'], ascending=[True, True, True])
            df = self.CommonFunction.reset_index(df)
            df['event_lower'] = df['event'].shift(+1)
            df['time_lower'] = df['time'].shift(+1)
            df['port_id_lower'] = df['port_id'].shift(+1)
            df['installation_id_lower'] = df['installation_id'].shift(+1)
            df['idle_time'] = np.nan
            df = self.CommonFunction.datediff_in_millisec(df, 'time', 'time_lower', 'idle_time')
        return df

    def port_unutilization_time_final1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("port_unutilization_time", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()
        #check_start_date = pd.to_datetime(self.start_date) - timedelta(minutes=5)
        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select *  from port_load_unload_events where ((event='bot_unloading_at_port_started' and time>'{self.start_date}') or (event='port_unloading_at_conveyor_completed' and time>'{self.start_date}')) and time <= '{self.end_date}' order by time desc"
        q1 = f"select *  from port_load_unload_events where ((event='port_lift_to_tray_transfer_completed' and time>'{self.start_date}')  or (event='port_to_bot_transfer_completed' and time>'{self.start_date}')) and port_type='exit' and time <= '{self.end_date}' order by time desc"
        q2 = f"select *  from last_port_unutilization_time order by time desc limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        df2 = pd.DataFrame(self.read_client.query(q1).get_points())
        df3 = pd.DataFrame(self.client.query(q2).get_points())
        if 'cron_ran_at' in df3.columns:
            cron_ran_at = df3['cron_ran_at'][0]
            q2 = f"select *  from last_port_unutilization_time where cron_ran_at = '{cron_ran_at}' order by time desc"
        else:
            q2 = f"select *  from last_port_unutilization_time order by time desc"
        df3 = pd.DataFrame(self.client.query(q2).get_points())
        if df.empty or df2.empty:
            filter = {"site": self.tenant_info['Name'], "table": "port_unutilization_time"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
        if not (df.empty or df2.empty):
            df_port_unloading_at_conveyor_completed = pd.DataFrame()
            df_port_to_bot_transfer_completed = pd.DataFrame()
            if not df3.empty:
                df3['Extra']=1
                df_port_to_bot_transfer_completed=df3[df3['event']=='port_to_bot_transfer_completed']
                df_port_unloading_at_conveyor_completed = df3[df3['event'] == 'port_unloading_at_conveyor_completed']
            df['Extra'] = 0
            df2['Extra'] = 0
            df= self.calculation_time(df,df_port_unloading_at_conveyor_completed)
            df2 = self.calculation_time(df2,df_port_to_bot_transfer_completed)
            if not df.empty:
                df['idle_time'] = df.apply(lambda x: x['idle_time'] if x['port_id'] == x['port_id_lower'] and x['installation_id'] == x['installation_id_lower'] and
                                                    x['event'] == 'bot_unloading_at_port_started' and
                                                    x['event_lower'] == 'port_unloading_at_conveyor_completed' else 0, axis=1)

            if not df2.empty:
                df2['idle_time']= df2.apply(lambda x:x['idle_time'] if x['port_id']==x['port_id_lower'] and x['installation_id'] == x['installation_id_lower'] and
                                                     x['event']=='port_lift_to_tray_transfer_completed' and
                                                    x['event_lower']=='port_to_bot_transfer_completed' else 0 ,axis =1)
            combine_df = pd.concat([df, df2])
            new_df = combine_df.groupby(['installation_id', 'port_id', 'event'],
                                        as_index=False).agg(time=('time', 'max'))
            combine_df=combine_df[combine_df['Extra']==0]
            if not combine_df.empty:
                l = ['event_lower', 'index', 'level_0', 'port_id_lower','time_lower','Extra','installation_id_lower']
                for col in l:
                    if col in combine_df.columns:
                        del combine_df[col]
            combine_df = self.CommonFunction.reset_index(combine_df)
            combine_df.time = pd.to_datetime(combine_df.time)
            combine_df['idle_time'] = combine_df['idle_time'].astype(float)
            combine_df['time_diff'] = combine_df['time_diff'].astype(float)
            combine_df['tote_count'] = combine_df['tote_count'].astype(float)
            combine_df = combine_df.set_index('time')
            new_df.time = pd.to_datetime(new_df.time)
            new_df = new_df.set_index('time')
            curr_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            new_df['cron_ran_at'] = curr_time
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(combine_df, "port_unutilization_time", db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['port_id'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            # self.client.query("drop measurement last_port_unutilization_time")
            self.write_client.writepoints(new_df, "last_port_unutilization_time",
                                          db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['port_id'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None


with DAG(
        'port_unutilization_time',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of picks per rack face GM-44025',
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
            if tenant['Active'] == "Y" and tenant['port_unutilization_time'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                final_task = PythonOperator(
                    task_id='port_unutilization_time_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(port_unutilization_time().port_unutilization_time_final,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='port_unutilization_time_final',
            provide_context=True,
            python_callable=functools.partial(port_unutilization_time().port_unutilization_time_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

