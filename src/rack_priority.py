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
import json
from pandas import json_normalize
from config import (
    rp_seven_days
)
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

class RackPriority:

    def rack_priority(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")
        self.read_client_platformbusinessstats = InfluxData(host=self.tenant_info["influx_ip"],
                                                                    port=self.tenant_info["influx_port"],
                                                                    db="PlatformBusinessStats")        

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date(f"{rp_seven_days}.rack_priority", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select * from rack_arrival_scheduler_run where  time>'{check_start_date}' and time<='{check_end_date}' order by time desc limit 1"
        df = pd.DataFrame(self.read_client_platformbusinessstats.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.rack_priority1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3(f"{rp_seven_days}.rack_priority", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.rack_priority1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def rack_priority1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date(f"{rp_seven_days}.rack_priority", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(self.start_date, self.end_date)

        q = f"select * from rack_arrival_scheduler_run where  time>'{self.start_date}' and time<='{self.end_date}' order by time desc"
        df = pd.DataFrame(self.read_client_platformbusinessstats.query(q).get_points())
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": f"{rp_seven_days}.rack_priority"}
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter, new_last_run)
            return
        df["outer_list"] = df["rack_arrival_details"].apply(json.loads)
        df_exploded = df.explode("outer_list")
        if 'rack_arrival_details' in df_exploded.columns:
            del df_exploded['rack_arrival_details']
        df_exploded=df_exploded.reset_index()
        df_exploded['row_number'] = range(1, len(df_exploded) + 1)
        rows = []
        for x in df_exploded.index:
            item= df_exploded["outer_list"][x]
            meta = {
                "row_number" : df_exploded["row_number"][x],
                "entityId": item["entityId"],
                "rackFace": item["rackFace"],
                "entityType": item["entityType"],
                "eta": item["eta"]
            }
            details_df = json_normalize(item["containerDetails"], sep="_")
            for k, v in meta.items():
                details_df[k] = v
            rows.append(details_df)

        final_df = pd.concat(rows, ignore_index=True)
        final_df.columns = final_df.columns.str.replace(r"\.", "_", regex=True)  
        df = pd.merge(df_exploded,final_df,on=['row_number'],how='inner')
        if 'outer_list' in df.columns:
            del df['outer_list']
        
        df['row_number'] = range(1, len(df) + 1)
        df['row_number'] = pd.to_numeric(df['row_number'], errors='coerce') 
        df['time'] = pd.to_datetime(df['time'].str.slice(0, 19), format='%Y-%m-%dT%H:%M:%S')
        df['time'] = df['time'] + pd.to_timedelta(df['row_number'], unit='s')
        df.time = pd.to_datetime(df['time'])
        df['cron_ran_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        df = df.set_index('time')
        print("inserted")
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "rack_priority", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=[],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy=rp_seven_days)
        return None


with DAG(
        'Rack_Priority_Analysis',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Analysis of rack priority',
        schedule_interval='27 * * * *',
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
            if tenant['Active'] == "Y" and tenant['rack_priority'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                try:
                    final_task = PythonOperator(
                        task_id='rack_priority_analysis_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(RackPriority().rack_priority,
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
            task_id='rack_priority_analysis',
            provide_context=True,
            python_callable=functools.partial(RackPriority().rack_priority,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

