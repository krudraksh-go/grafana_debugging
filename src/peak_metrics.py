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
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
from r2r_waittime_calculation import R2rWaittimeCalculation
from operator_working_time_summary import operator_working_time_summary
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

class PeakMetric:

    def get_datetime_interval(self):
        site = self.tenant_info['Name']
        q = f"select * from peak_metrics where site='{site}'  order by time desc limit 1"
        central_influx_client = self.CommonFunction.get_central_influx_client(self.tenant_info.get('alteryx_out_db_name'))
        start_datetime = central_influx_client.fetch_one(q)
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
            start_datetime =start_datetime+timedelta(days=1)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=10)
        start_time = self.tenant_info['StartTime']
        hour, minute, second = map(int, start_time.split(':'))
        start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = datetime.now(timezone.utc)
        timeseriesdata = self.CommonFunction.create_time_series_data2(start_datetime,end_datetime, '1d')
        return timeseriesdata    

    def log_peak_metrics(self, tenant_info, **kwargs):
        self.CommonFunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        daterange = self.get_datetime_interval()
        for i in daterange.index:
            self.start_date = daterange['interval_start'][i]
            self.log_peak_metrics1(self.start_date, **kwargs)
        
    def log_peak_metrics1(self, start_date, **kwargs):
        self.start_date = start_date
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        self.utilfunction = CommonFunction()
        df = self.utilfunction.get_sheet_from_airflow_setup_excel('expected_values')   
        site = self.tenant_info['Name']
        df = df[df['Name'] == site]
        columns = df.columns.tolist()
        final_column = []
        for column in columns:
            if 'sol' in column:
                final_column.append(column)
        df = df[final_column]
        df = df.T.reset_index()
        df.columns = ['metric', 'value']
        df['value'] =  df['value'].astype(str)
        df['peak'] = df['value'].apply(lambda x: x.split('|')[0])
        df['non_peak'] = df['value'].apply(lambda x: x.split('|')[1] if (len(x.split('|')) > 1) else 0)
        df = df.replace('NA', '0')
        df['peak'] = df['peak'].astype(float)
        df['non_peak'] = df['non_peak'].astype(float)
        df['day_type'] = 'non_peak'
        df['metric'] = df['metric'].apply(lambda x: x.replace('sol__', ''))
        df['metric'] = df['metric'].apply(lambda x: x.replace('sol_', ''))
        df['flow'] = df['metric'].apply(lambda x: x.split('__')[1].upper() if '__' in x else 'RTP')
        df['type'] = df['metric'].apply(lambda x: x.split('__')[2].upper() if len(x.split('__')) > 2 else 'default')
        df['metric'] = df['metric'].apply(lambda x: x.split('__')[0])
        peak_date_df = self.utilfunction.get_sheet_from_airflow_setup_excel('peak_dates')
        peak_date_df = peak_date_df[peak_date_df['Name'] == site]
        peak_date_df['peak_start'] = pd.to_datetime(peak_date_df['peak_start']).dt.tz_localize('UTC')
        peak_date_df['peak_end'] = pd.to_datetime(peak_date_df['peak_end']).dt.tz_localize('UTC')
        today_date = pd.to_datetime(self.start_date)
        for index, row in peak_date_df.iterrows():
            if today_date >= row['peak_start'] and today_date <= row['peak_end']:
                df['day_type'] = 'peak'
        df['site'] = site
        if 'value' in df.columns:
            del df['value']
        df['time'] = pd.to_datetime(self.start_date)
        df = df.set_index('time')
        df = df.fillna(0)
        df['peak'] = df['peak'].astype(float)
        df['non_peak'] = df['non_peak'].astype(float)
        df['key'] = pd.to_datetime(self.start_date).strftime('%Y-%m-%d') + '_' + df['site'] + '_' + df['flow'] + '_' + df['metric']

        write_influx = self.CommonFunction.get_central_influx_client(self.tenant_info["alteryx_out_db_name"])
        isvalid= write_influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if isvalid:
            success = write_influx.write_all(df, 'peak_metrics', dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], tag_columns=['metric','type','flow'])
            print("Business metrics calculation inserted")
        return None


with DAG(
        'peak_metrics',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Peak Metric Info Logged In Influx',
        schedule_interval='0 11 * * *',
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
            if tenant['Active'] == "Y" and tenant['peak_metrics'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='peak_metrics_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(PeakMetric().log_peak_metrics,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=600),
                    )
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='peak_metrics_final',
            provide_context=True,
            python_callable=functools.partial(PeakMetric().log_peak_metrics,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

