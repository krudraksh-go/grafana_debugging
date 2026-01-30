import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, postgres_connection
from dynamic_insights import DynamicInsights

class R2R_insights:

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df

    def final_call(self, tenant_info, **kwargs):
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

        check_start_date = self.client.get_start_date("debugging_insight", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date1 = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date1 = pd.to_datetime(check_start_date1).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>'{check_start_date1}' and time<='{check_end_date}' and value > 0 limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.start_date = pd.to_datetime(check_start_date)
            self.end_date = datetime.now(timezone.utc)
            self.final_call_1(self.end_date, **kwargs)
        else:
            try:
                check_start_date = pd.to_datetime(check_start_date, utc=True) - timedelta(minutes=90)
                end_datetime = datetime.now(timezone.utc)
                daterange = self.create_time_series_data2(check_start_date, end_datetime, '1h')
                if daterange.empty:
                    daterange = self.create_time_series_data2(check_start_date, end_datetime, '15min')
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.start_date = daterange['start_date'][i]
                    self.final_call_1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call_1(self, end_date, **kwargs):
        site_name = self.tenant_info["Name"]
        self.utilfunction = CommonFunction()
        self.start_date = self.start_date.strftime("%Y-%m-%d %H:%M:00")
        self.end_date = self.end_date.strftime("%Y-%m-%d %H:%M:00")
        obj = DynamicInsights(self.start_date, self.end_date,self.tenant_info)
        df= obj.main()
        #print(df)
        df['actual_value'] = df['actual_value'].astype(str,errors='ignore')
        df['expected_value'] = df['expected_value'].astype(str,errors='ignore')
        df['actual_value'] = df['actual_value'].apply(lambda x: "-" if x=="nan%" or x=="nan" or pd.isna(x) else x)
       # df['actual_value'] = df['actual_value'].round(2)
        if 'index' in df:
            del df['index']
        # df.time = self.end_date
        self.end_date = pd.to_datetime(self.end_date)
        # df = df.set_index(pd.to_datetime(np.arange(len(df))*-1, unit='ns', origin=self.end_date))
        # df = df.set_index('time')
        df['time']=self.end_date
        df.time = pd.to_datetime(np.arange(len(df)) * -1, unit='s', origin=self.end_date)
        # df['pps_id']=df.apply(lambda x:'0' if x['per_pps']=='NO' else x['pps_id'], axis=1)
        df['pps_id'] = df['pps_id'].astype(str,errors='ignore')
        df = df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "debugging_insight", db_name=self.tenant_info["alteryx_out_db_name"],
                                      tag_columns=['insights','parameter'], dag_name=os.path.basename(__file__),
                                      site_name=self.tenant_info['Name'])


with DAG(
        'Dynamic_insights',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Gives insights about high R2R time',
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
            if tenant['Active'] == "Y" and tenant['r2r_insights'] == "Y":
                final_task = PythonOperator(
                    task_id='r2r_insights_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(R2R_insights().final_call, tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='r2r_insights_final',
            provide_context=True,
            python_callable=functools.partial(R2R_insights().final_call, tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )
