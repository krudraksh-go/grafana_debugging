## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, postgres_connection
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

class PbtDifferenceCalculator:

    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        #        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")
        self.utilfunction = CommonFunction()

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("pbt_difference", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        self.postgres_conn = postgres_connection(database='platform_srms',
                                                 user=self.tenant_info['Postgres_pf_user'], \
                                                 sslrootcert=self.tenant_info['sslrootcert'],
                                                 sslcert=self.tenant_info['sslcert'], \
                                                 sslkey=self.tenant_info['sslkey'],
                                                 host=self.tenant_info['Postgres_pf'], \
                                                 port=self.tenant_info['Postgres_pf_port'],
                                                 password=self.tenant_info['Postgres_pf_password'], \
                                                 dag_name=os.path.basename(__file__), \
                                                 site_name=self.tenant_info['Name'])
        # cursor = self.postgres_conn.pg_curr
        q = f"SELECT * from service_request where updated_on > '{check_start_date}' and updated_on <= '{check_end_date}' and status = 'PROCESSED' and state = 'released' and type = 'PICK' and attributes::json->>'is_short_pick'!='true' limit 1"
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        self.postgres_conn.close()
        if not data:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.utilfunction.create_time_series_data2(check_start_date, check_end_date, '1h')
                for i in daterange.index:
                    end_date = daterange['interval_end'][i]
                    self.final_call1(end_date)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e


    def final_call1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("pbt_difference", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date: {self.end_date}")

        self.postgres_conn = postgres_connection(database='platform_srms',
                                                 user=self.tenant_info['Postgres_pf_user'], \
                                                 sslrootcert=self.tenant_info['sslrootcert'],
                                                 sslcert=self.tenant_info['sslcert'], \
                                                 sslkey=self.tenant_info['sslkey'],
                                                 host=self.tenant_info['Postgres_pf'], \
                                                 port=self.tenant_info['Postgres_pf_port'],
                                                 password=self.tenant_info['Postgres_pf_password'], \
                                                 dag_name=os.path.basename(__file__), \
                                                 site_name=self.tenant_info['Name'])
        # cursor = self.postgres_conn.pg_curr
        q = f"SELECT id,external_service_request_id, updated_on,attributes::json->>'order_date' as drop_date,created_on,attributes::json->>'released_time' as completion_time, attributes::json->>'pick_before_time' as pbt from service_request where updated_on > '{self.start_date}' and updated_on <= '{self.end_date}' and status = 'PROCESSED' and state = 'released' and type = 'PICK' and attributes::json->>'is_short_pick'!='true' order by updated_on"
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        self.postgres_conn.close()
        cols = ['id','external_service_request_id','updated_on', 'drop_date', 'created_on', 'completion_time', 'pbt']
        df = pd.DataFrame(data,columns=cols)
        if df.empty:
            self.utilfunction.update_dag_last_run({"site": self.tenant_info['Name'], "table": "pbt_difference"},
                                                  {"last_run": self.end_date})
            return None
        cols = [col for col in cols if col not in ('id', 'external_service_request_id')]

        for col in cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col],utc=True)
            
        cols_to_create = ['drop_to_created', 'created_to_complete', 'created_to_pbt']
        for col in cols_to_create:
            if col not in df.columns:
                df[col] = np.nan
        df = self.utilfunction.datediff_in_millisec(df, 'created_on','drop_date','drop_to_created')
        df = self.utilfunction.datediff_in_millisec(df, 'completion_time','created_on','created_to_complete')
        df = self.utilfunction.datediff_in_millisec(df, 'pbt','created_on','created_to_pbt')

        for col in cols_to_create:
            if col in df.columns:
                df[col] = df[col].astype('float', errors='ignore')
        df['completion_time'] = df['completion_time'].apply(lambda x : pd.to_datetime(x.replace(second=0).strftime('%Y-%m-%dT%H:%M:%SZ')))
        df['pbt'] = df['pbt'].apply(lambda x : pd.to_datetime(x.replace(second=0).strftime('%Y-%m-%dT%H:%M:%SZ')))
        df['breached'] = 'false'
        df['breached'] = df.apply(lambda x: 'true' if  (x['pbt'] < x['completion_time']) else 'false', axis=1)
        df['breached'] = df['breached'].astype('str')
        df['prev_updated_on'] = df['updated_on'].shift(1)
        df = df.reset_index()
        df['updated_on'] = df.apply(lambda x : x['updated_on'] + timedelta(milliseconds = x['index']) if (x['updated_on']==x['prev_updated_on']) else x['updated_on'], axis=1)

        final_cols = ['external_service_request_id','updated_on','breached', 'drop_to_created', 'created_to_complete', 'created_to_pbt', 'drop_date', 'created_on', 'completion_time', 'pbt']
        df = df[final_cols]

        df = df.rename(columns={'updated_on': 'time'})
        df['host'] = self.tenant_info['host_name']
        df['installation_id'] = self.tenant_info['installation_id']
        df['host'] = df['host'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)

        df = df.set_index('time')

        for col in cols:
            if col in df.columns:
                df[col] = df[col].astype('str', errors='ignore')        

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "pbt_difference", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=[],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        print("inserted")
        return None


with DAG(
        'pbt_difference',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Pick to Pbt Difference Calculation',
        schedule_interval='48 * * * *',
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
            if tenant['Active'] == "Y" and tenant['pbt_difference'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='pbt_difference_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(PbtDifferenceCalculator().final_call,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=3600) )
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='picks_per_rack_final',
            provide_context=True,
            python_callable=functools.partial(PbtDifferenceCalculator().final_call,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

