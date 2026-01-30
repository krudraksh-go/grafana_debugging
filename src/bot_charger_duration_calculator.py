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
import numpy as np
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

class BotChargerDurationCalculator:

    def compute_bot_charger_durations(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("bot_charger_usage", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from chargetask_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.compute_bot_charger_durations1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("bot_charger_usage", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.compute_bot_charger_durations1(self.end_date, **kwargs)
                    # break
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def compute_bot_charger_durations1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("bot_charger_usage", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(self.start_date, self.end_date)

        q = f"select * from chargetask_events where time>'{self.start_date }' and time <= '{self.end_date}' and event!='update_ampere_hours' order by time desc"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "bot_charger_usage"}
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter, new_last_run)

        one_day_before = (pd.to_datetime(self.end_date) - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select * from bot_charger_usage where time>'{one_day_before}' and status!='success'"
        incomplete_df = pd.DataFrame(self.client.query(q).get_points())
        cols = ['time','butler_id','charger_id','start_time','end_time','time_diff','status','reason','taskkey','installation_id']
        if not incomplete_df.empty:
            incomplete_df = incomplete_df[cols]

            incomplete_df = incomplete_df.fillna('')
        df = df.fillna('')
        df['end_time'] = df.apply(lambda x: x['time'] if x['event'] == 'stop_charging' else '', axis = 1)
        df['cycle_start'] = df['event'].apply(lambda x: 1 if x in ['reached_reinit_point','charger_ready','docking_success','contactor_on'] else 0)
        df = df.groupby(['installation_id','taskkey', 'butler_id','charger_id'], as_index=False).agg(
            time= ('time', 'min'),
            last_time= ('time', 'max'),
            start_time= ('time', 'min'),    
            end_time= ('end_time', 'max'),
            reason= ('reason', 'max'),
            status= ('status', 'max'),
            cycle_start = ('cycle_start','max'))
        df['status'] = df['status'].apply(lambda x : 'in progress' if x=='' else x)
        df = df[(df['status']!='in progress') | (df['cycle_start']==1)].reset_index(drop=True)
        df['end_time'] = df.apply(lambda x : x['last_time'] if x['end_time'] =='' else x['end_time'], axis = 1)
        df = pd.concat([df,incomplete_df]).reset_index(drop=True)
        df = df.groupby(['installation_id','taskkey', 'butler_id','charger_id'], as_index=False).agg(
            time= ('time', 'min'),
            start_time= ('time', 'min'),    
            end_time= ('end_time', 'max'),
            reason= ('reason', 'max'),
            status= ('status', 'max'))
        df['time_diff'] = np.nan
        df  = self.CommonFunction.datediff_in_millisec(df, 'end_time', 'start_time', 'time_diff')
        df = df[cols]
        str_cols = ['butler_id','charger_id', 'start_time','end_time','status','reason','taskkey','installation_id']
        float_cols = ['time_diff']
        df[float_cols] = df[float_cols].fillna(0)
        df = self.CommonFunction.str_typecast(df,str_cols)
        df = self.CommonFunction.float_typecast(df,float_cols)
        df['time'] = pd.to_datetime(df['time'])
        df = df.set_index('time')
        print("inserted")
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "bot_charger_usage", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['charger_id'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        filter = {"site": self.tenant_info['Name'], "table": "bot_charger_usage"}
        new_last_run = {"last_run": self.end_date}
        self.CommonFunction.update_dag_last_run(filter, new_last_run)
        return None


with DAG(
        'Bot_charger_duration_calculator',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of time spend by bot in charger',
        schedule_interval='21 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(seconds=1200),
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['bot_charger_duration'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                try:
                    final_task = PythonOperator(
                        task_id='bot_charger_duration_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(BotChargerDurationCalculator().compute_bot_charger_durations,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=3600),
                    )
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='bot_charger_duration_final',
            provide_context=True,
            python_callable=functools.partial(BotChargerDurationCalculator().compute_bot_charger_durations,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

