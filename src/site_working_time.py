## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import pytz
import math
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

class site_working_hours:
    def ceil_data(self, x):
        diff = (
                       (x['time'].tz_convert(tz=pytz.UTC)) - pd.to_datetime(self.start_date,utc=True).tz_convert(tz=pytz.UTC)).total_seconds() / 60.0
        # diff = (x['time']-x['interval_start_date'].tz_localize(tz=pytz.UTC)).total_seconds() / 60.0
        # diff= self.value_in_sec(x['time'],x['interval_start_date']).total_seconds() / 60.0
        val = math.ceil((diff) / 5)
        if val == 0:
            val = 1
        return val


    def site_working_hours_final(self, tenant_info, **kwargs):
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

        check_start_date = self.client.get_start_date("site_working_time", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>'{check_start_date}' and time<='{check_end_date}' and value > 0 limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.site_working_hours_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("site_working_time", '1d', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.site_working_hours_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def site_working_hours_final1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("site_working_time", self.tenant_info)
        self.end_date = end_date

        #self.end_date = self.end_date.replace(second=0)
        self.utilfunction = CommonFunction()
      #  self.start_date = self.start_date.replace(second=0, microsecond=0)
        hour, minute, second = map(int, self.tenant_info["StartTime"].split(':'))
        self.start_date=pd.to_datetime(self.start_date, utc=True)
        self.start_date = self.start_date.replace(hour=hour, minute=minute, second=0, microsecond=0)

        self.end_date = self.end_date.replace(hour=hour, minute=minute, second=0, microsecond=0)

        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"self.start_date:self.end_date:{self.start_date}:{self.end_date}")

        q = f"select * from pps_data where time>'{self.start_date}' and time <= '{self.end_date}' and status='open' and front_logged_in='true' order by time desc"
        df_main = pd.DataFrame(self.read_client.query(q).get_points())
        # temp_start_date = self.start_date.replace(second=0, microsecond=0)
        # interval_end_date = self.end_date.replace(second=0, microsecond=0)
        if not df_main.empty:
            timeseriesdata = self.utilfunction.create_time_series_data(pd.to_datetime(self.start_date,utc=True), pd.to_datetime(self.end_date,utc=True), '5min')
            # df_main['date'] = df_main.time.apply(
            #     lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').date() if len(x) > 20 else datetime.strptime(x,
            #                                                                                                          '%Y-%m-%dT%H:%M:%SZ').date())
            df_main['time'] = pd.to_datetime(df_main['time'], utc=True)
            df_main['ceil'] = df_main.apply(self.ceil_data, axis=1)
            df_main = pd.merge(df_main, timeseriesdata, how='left', on='ceil')
            df_main=df_main[~pd.isna(df_main['interval_start'])]
            df_main2 = df_main.groupby(['installation_id'], as_index=False).agg(time=('time', 'min'),
                                                                                          start_time=('time', 'min'),
                                                                                          end_time=('time', 'max'),
                                                                                          working_hr=('ceil', 'nunique'))
            df_main2['working_hr'] = df_main2.working_hr.apply(lambda x: float(x) * 5 / 60)
            df_main2.start_time = df_main2.start_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f%z'))
            df_main2.end_time = df_main2.end_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f%z'))
            df_main2['working_hr'] = df_main2['working_hr'].astype(float)
            df_main2['installation_id'] = df_main2['installation_id'].astype('str')
            df_main2.time = pd.to_datetime(self.end_date,utc=True)
            df_main2 = df_main2.set_index('time')
            # if 'date' in df_main2.columns:
            #     del df_main2['date']
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df_main2, "site_working_time", db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=[],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None


with DAG(
        'site_working_hours',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of picks per rack face GM-44025',
        schedule_interval='0 0 * * *',
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
            if tenant['Active'] == "Y" and tenant['site_working_hours'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                final_task = PythonOperator(
                    task_id='site_working_hours_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(site_working_hours().site_working_hours_final,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='site_working_hours_final',
            provide_context=True,
            python_callable=functools.partial(site_working_hours().site_working_hours_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

