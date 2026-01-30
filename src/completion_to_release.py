import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
from pandasql import sqldf
import pytz
import time
import numpy as np
import os
import psycopg2


class CompletionToRelease():
    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")
        self.utilfunction = CommonFunction()

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("completion_to_release", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select * from order_status_log  where  time >= '{check_start_date}' and time < '{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())

        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("completion_to_release", '4h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("completion_to_release", '1h', self.tenant_info)

                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e
        # self.final_call1(datetime.now(timezone.utc))

    def final_call1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("completion_to_release", self.tenant_info)
        # self.start_date = datetime.now()-timedelta(hours=1)
        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.start_date = pd.to_datetime(self.start_date, utc=True)
        self.start_date_60min_delay = self.start_date-timedelta(hours=1)
        self.start_date = self.start_date.replace(second=0, microsecond=0)
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.start_date_60min_delay = self.start_date_60min_delay.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        q = f"select * from order_status_log where time>='{self.start_date_60min_delay}' and time<='{self.end_date}' order by time desc"
        df1 = pd.DataFrame(self.read_client.query(q).get_points())
        if not df1.empty:
            df1.drop(['host', 'value', 'order_status'], axis=1, inplace=True)

        q = f"select installation_id,order_id,value from item_picked where time>='{self.start_date_60min_delay}' and time<='{self.end_date}' order by time desc"
        df_item = pd.DataFrame(self.read_client.query(q).get_points())
        if not df_item.empty:
            df_item = df_item.groupby(['installation_id', 'order_id'], as_index=False).agg(line_count=('order_id', 'count'),
                                                total_qty=('value', 'sum')
                                                )

        q = f"select * from ppsbin_log where time>='{self.start_date}' and time<='{self.end_date}' order by time desc"
        df2 = pd.DataFrame(self.read_client.query(q).get_points())
        if not df2.empty and not df1.empty and not df_item.empty:
            df2.drop(['host', 'value', 'bin_status'], axis=1, inplace=True)

            df2 = pd.merge(df2, df_item, how='left', on=['installation_id', 'order_id'])

            merge_df = pd.merge(df1, df2, how='inner', on=['pps_id', 'installation_id', 'order_id', 'bin_id'])
            # df = merge_df.groupby(['installation_id', 'pps_id', 'bin_id', 'time_y']).agg(
            #     {'order_id': 'count', 'time_x': 'max','line_count':'max','total_qty':'max'}).reset_index()
            # df.rename({'time_y': 'free_time', 'order_id': 'no_of_order_processed', 'time_x': 'completion_time'}, axis=1,
            #           inplace=True)

            df = merge_df.groupby(['installation_id', 'pps_id', 'bin_id', 'time_y'], as_index=False).agg(
                no_of_order_processed=('order_id', 'count'),
                order_id=('order_id', 'max'),
                completion_time=('time_x', 'max'),
                line_count=('line_count','max'),
                total_qty=('total_qty','max')
                )
            df.rename({'time_y': 'free_time'}, axis=1, inplace=True)

            df['completion_to_released'] = None
            if not df.empty:
                df = self.utilfunction.datediff_in_sec(df, 'free_time', 'completion_time', 'completion_to_released')

                cron_run_at = self.end_date
                df['cron_ran_at'] = cron_run_at
                df = self.utilfunction.reset_index(df)
                df['time'] = pd.to_datetime(df['free_time'])
                #df = self.utilfunction.get_epoch_time(df, date1=cron_run_at)
                #df['time'] = pd.to_datetime(df.time)
                #df.drop(['level_0', 'index'], axis=1, inplace=True)
                if not df.empty:
                    l = ['level_0', 'index']
                    for col in l:
                        if col in df.columns:
                            del df[col]

                df = df[['time', 'bin_id', 'completion_time', 'completion_to_released', 'cron_ran_at', 'free_time',
                         'no_of_order_processed', 'pps_id','line_count','total_qty','order_id','installation_id']]

                df['line_count'] = df['line_count'].astype(float)
                df['total_qty'] = df['total_qty'].astype(float)
                df['order_id'] = df['order_id'].astype(str)
                df.time = pd.to_datetime(df.time)
                df = df.set_index('time')
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                     port=self.tenant_info["write_influx_port"])
                self.write_client.writepoints(df, "completion_to_release", db_name=self.tenant_info["alteryx_out_db_name"],
                                              tag_columns=['pps_id'], dag_name=os.path.basename(__file__),
                                              site_name=self.tenant_info['Name'])
        else:
            filter = {"site": self.tenant_info['Name'], "table": "completion_to_release"}
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter, new_last_run)

        return None


with DAG(
        'Completion_to_release',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Completion_to_release',
        schedule_interval='20 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['Completion_to_release'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Completion_to_release_final_task = PythonOperator(
                    task_id='Completion_to_release_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(CompletionToRelease().final_call,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":"dev_postgres", "influx_ip":"dev_influxdb", "influx_port":8086,\
        #           "write_influx_ip":"dev_influxdb","write_influx_port":8086, \
        #           "out_db_name":"airflow", "alteryx_out_db_name": "airflow"}
        tenant = CommonFunction().get_tenant_info()
        Completion_to_release_final_task = PythonOperator(
            task_id='Completion_to_release_final',
            provide_context=True,
            python_callable=functools.partial(CompletionToRelease().final_call, tenant_info={'tenant_info': tenant}),
            # op_kwargs={
            #     'tenant_info': tenant,
            # },
            execution_timeout=timedelta(seconds=3600),
        )
