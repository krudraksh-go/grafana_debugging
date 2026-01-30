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
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, postgres_connection
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os

from config import (
    rp_seven_days
)

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class priority_change:

    def convert_to_ns(self, df,col):
        df[col] = df[col].apply(lambda x: x.timestamp())
        df[col] = df[col]* 1e3
        return df    

    def convert_to_dtype(self, df , cols, dtype, fill_value):
        df[cols] = df[cols].fillna(value = fill_value)
        df[cols] = df[cols].replace('None', fill_value)
        df[cols] = df[cols].replace('', fill_value)
        df[cols] = df[cols].replace('-', fill_value)
        
        for col in cols:
            df[col] = df[col].astype(dtype, errors='ignore')
        return df

    def priority_change_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"]) 

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        self.start_date = self.client.get_start_date(f"{rp_seven_days}.priority_change", self.tenant_info)
        self.end_date = datetime.now(timezone.utc)
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        self.CommonFunction = CommonFunction()

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


        q = f"""select external_service_request_id,  ((attributes::json->>'order_options')::json->>'bintags')::json->>1 as bin_tags,
                attributes::json->>'simple_priority' as priority, \
                created_on, updated_on,attributes::json->>'pick_before_time' as pbt, id, status, \
                state, attributes::json->>'released_time'  as released_time, attributes::json->>'completion_time'  as completion_time \
                from service_request \
                WHERE updated_on>'{self.start_date}' and updated_on<'{self.end_date}' and attributes::json->>'simple_priority'='critical' and "type" = 'PICK'  """
        print(q)
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        df = pd.DataFrame(data, columns=[col[0] for col in self.postgres_conn.pg_curr.description])
        self.postgres_conn.close()
        if df.empty:
            print("No data in service request")
            filter = {"site": self.tenant_info['Name'], "table": f"{rp_seven_days}.priority_change"}
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter, new_last_run)
            return None
        df['time'] = pd.to_datetime(df['updated_on']) + pd.to_timedelta(df['id'].astype(int), unit='ns')
        df = df.set_index('time')      
        df['updated_on_first'] = df['updated_on']
        epoch_time_fields = ['created_on','updated_on','pbt','released_time','completion_time','updated_on_first']
        zero_datetime = pd.to_datetime(datetime.utcfromtimestamp(0).strftime('%Y-%m-%d %H:%M:%S'))
        for col in epoch_time_fields:
            df[col] = df[col].apply(lambda x: pd.to_datetime(x) if (not pd.isna(x) and x!= '') else zero_datetime)
            df = self.convert_to_ns(df,col)
        df = self.convert_to_dtype(df, epoch_time_fields, int, 0)
        df['inital_priority']= df.apply(lambda x: x['priority'] if x['created_on']==x['updated_on'] else '-', axis=1)
        str_columns = ['priority','id','status','state','external_service_request_id','bin_tags','inital_priority']
        df = self.convert_to_dtype(df, str_columns, str, '-')          

        df['host'] = self.tenant_info['host_name']
        df['installation_id'] = self.tenant_info['installation_id']
        df['host'] = df['host'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "priority_change", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=[],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy=rp_seven_days)
        return None


with DAG(
        'priority_change',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='priority_change dag created',
        schedule_interval='39 * * * *',
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
            if tenant['Active'] == "Y" and tenant['priority_change'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='priority_change_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(priority_change().priority_change_final,
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
            task_id='priority_change_final',
            provide_context=True,
            python_callable=functools.partial(priority_change().priority_change_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

