import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import json

import gspread
import pytz
import numpy as np
from math import ceil

# import airflow
# from airflow import DAG
from utils.CommonFunction import SingleTenantDAGBase, MultiTenantDAG,CommonFunction
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone
from daily_site_kpi import DailySiteKPI
from all_site_kpi import AllSiteKPI
from one_view import OneViewAYX
from business_metrics_calculator import MetricCalculator
from create_consolidate_report import consolidatareport
from config import (
    INFLUX_DATETIME_FORMAT,
)

####################### Config #######################


dag = DAG(
    "kpi_generation",
    start_date=datetime.strptime('04/23/23 13:55:26', '%m/%d/%y %H:%M:%S'),
    schedule_interval="30 8,9,10,23,0 * * *",
    default_args=CommonFunction().get_default_args_for_dag(),
    catchup=False,
)

class kpi_generation(SingleTenantDAGBase):

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df

    def get_datetime_interval2(self):
        influx = self.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        start_datetime = influx.fetch_one("""select * from OneView_data_v2 
                      where site='{site}'  order by time desc limit 1""".format(
            site=self.site.get('name')))
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
            start_datetime =start_datetime+timedelta(days=1)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=3)
        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = datetime.now(timezone.utc)

        timeseriesdata = self.create_time_series_data2(start_datetime,end_datetime, '1d')
        return timeseriesdata

    def FuncConsolidateData(self, **kwargs):
        daterange = self.get_datetime_interval2()
        print(daterange)
        for i in daterange.index:
            self.start_date=daterange['start_date'][i]
            self.end_date=daterange['end_date'][i]
            print(f"KPI:start_date:{self.start_date}, end_date:{self.end_date}")
            d =DailySiteKPI()
            d.site=self.site
            a= AllSiteKPI()
            a.site = self.site
            o=OneViewAYX()
            o.site=self.site

            c =consolidatareport()
            c.site=self.site
            m = MetricCalculator()
            m.site = self.site
            # d.start_date= self.start_date
            # a.start_date = self.start_date
            # o.start_date = self.start_date
            # d.end_date= self.end_date
            # a.end_date = self.end_date
            # o.end_date = self.end_date
            print("daily_site_kpi_by_kpi")
            d.daily_site_kpi_by_kpi(end_date=self.end_date, **kwargs)
            print("all_site_kpi_by_kpi")
            a.all_site_kpi_by_kpi(end_date=self.end_date,**kwargs)
            print("OneView_ayx_by_kpi")
            o.OneView_ayx_by_kpi(end_date=self.end_date)
            c.consolidate_kpi(start_date=self.start_date, end_date=self.end_date)
            m.final_call()
        return

MultiTenantDAG(
    dag,
    [
        'FuncConsolidateData',
    ],
    [],
    kpi_generation
).create()

# with DAG(
#     'kpi_generation',
#     default_args = default_args,
#     description = 'Gkpi_generation',
#     schedule_interval = '2 7 */1 * *',
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# ) as dag:
#    import csv
#    import os
#    import functools
#    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
#         csvReader = CommonFunction().get_all_site_data_config()
#         for tenant in csvReader:
#             if tenant['Active'] == "Y" and tenant['kpi_generation'] == "Y":
#                 daily_task = PythonOperator(
#                     task_id='daily_kpi_generation_final_data_{}'.format(tenant['Name']),
#                     provide_context=True,
#                     python_callable=functools.partial(DailySiteKPI().daily_site_kpi,tenant_info={'tenant_info': tenant}),
#                     execution_timeout=timedelta(seconds=3600),
#                 )
#
#                 allsite_task = PythonOperator(
#                     task_id='all_kpi_generation_final_data_{}'.format(tenant['Name']),
#                     provide_context=True,
#                     python_callable=functools.partial(AllSiteKPI().all_site_kpi,tenant_info={'tenant_info': tenant}),
#                     execution_timeout=timedelta(seconds=3600),
#                 )
#
#                 daily_task = PythonOperator(
#                     task_id='oneview kpi_generation_final_data_{}'.format(tenant['Name']),
#                     provide_context=True,
#                     python_callable=functools.partial(OneViewAYX().OneView_ayx,tenant_info={'tenant_info': tenant}),
#                     execution_timeout=timedelta(seconds=3600),
#                 )
#
#     else:
#         # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
#         #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
#         #           "out_db_name":db_name}
#         tenant = CommonFunction().get_tenant_info()
#         final_task = PythonOperator(
#             task_id='grid_barcode_mapping_final_data',
#             provide_context=True,
#             python_callable=functools.partial(DailySiteKPI().DailySiteKPI,tenant_info={'tenant_info': tenant}),
#             execution_timeout=timedelta(seconds=3600),
#         )
#
