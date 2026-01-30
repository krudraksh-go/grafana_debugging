## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, Elasticsearch
import requests

## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name =os.environ.get('Out_db_name', 'airflow')
from config import (
    rp_seven_days
)


dag = DAG(
    'slot_storage_utilization',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'slot_storage_utilization',
    schedule_interval = timedelta(days=1),
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
)

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class slot_storage_utilization:
    def add_additional_fields(self, data_list):
        sku_limit_status = lambda limit, count: 'Non-Breached' if limit == 'inf' or count < limit else 'Equal'
        utilization_threshold = lambda util: (
            'Green' if util <= 25.0 else 'Yellow' if util < 40.0 else 'Red'
        )
        free_slot = lambda count: 'Yes' if count == 0 else 'No'
        slot_blocked = lambda reason: 'False' if reason == '-' else 'True'

        df = pd.DataFrame([
            {**hit['_source']}
            for hit in data_list
        ])
        df['sku_limit_status'] = df.apply(lambda x:sku_limit_status(x['unique_sku_limit'], x['unique_sku_count']),axis=1)
        df['utilization_threshold'] = df.apply(lambda x: utilization_threshold(float(x['slot_utilization'])), axis=1)
        df['free_slot'] = df.apply(lambda x: free_slot(x['unique_sku_count']), axis=1)
        df['slot_blocked'] = df.apply(lambda x: slot_blocked(x['blocked_reason']), axis=1)

        return df

    def consolidate_df(self, df):
        string_mandatory_columns = ['volume_uom','weight_uom','type']
        for col in string_mandatory_columns:
            if col not in df.columns:
                df[col] = '-'      
        float_columns = ['free_volume','slot_weight','utilised_volume','volume','slot_utilization']  
        for col in float_columns:
            df[col] = df[col].astype(float)
        result = df.groupby(['rack_id','floor_id','tags']).agg(
            time = ('time','first'),
            cron_ran_at = ('cron_ran_at','first'),
            free_volume = ('free_volume','sum'),
            height = ('height','first'),
            length = ('length','first'),
            rack_type = ('rack_type','first'),
            weight = ('slot_weight','sum'),
            type = ('type','first'),
            utilised_volume = ('utilised_volume','sum'),
            volume = ('volume','sum'),
            volume_uom = ('volume_uom','first'),
            weight_uom = ('weight_uom','first'),
            width = ('width','first'),
            slot_utilization = ('slot_utilization','mean')
        ).reset_index() 
        result = result.set_index('time')
        
        return result


    def slot_storage_utilization_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["out_db_name"], timeout=600)
        elastic_client = Elasticsearch(host=self.tenant_info["elastic_ip"], port=self.tenant_info["elastic_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info["Name"])
        all_hits = elastic_client.fetch_all_docs('slot_storage_utilization', batch_size=5000)
        print(f"Total hits: {len(all_hits)}")
        df = self.add_additional_fields(all_hits)
        #df = pd.DataFrame(modified_hits)
        date_now = datetime.now(timezone.utc)
        date_now = date_now.replace(microsecond=0)
        df['cron_ran_at'] = date_now
        df.cron_ran_at = df.cron_ran_at.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        final_df = CommonFunction().get_epoch_time(df,date1=date_now)
        final_df['time'] = pd.to_datetime(final_df.time_ns)
        final_df = final_df.drop(columns=['time_ns','index'])
        consolidated_df = self.consolidate_df(final_df)
        final_df['host'] = self.tenant_info['host_name']
        final_df['installation_id'] = self.tenant_info['installation_id']
        final_df['host'] = final_df['host'].astype(str)
        final_df['installation_id'] = final_df['installation_id'].astype(str)
        #final_df.cron_ran_at = final_df.cron_ran_at.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        final_df = final_df.set_index('time')
        write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
        # try:
        #     self.client.query("drop measurement elastic_slot_storage_utilization")
        # except:
        #     pass
        write_client.writepoints(final_df, "elastic_slot_storage_utilization", db_name=self.tenant_info["out_db_name"], tag_columns=['rack_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'],retention_policy =rp_seven_days)
        write_client.writepoints(consolidated_df, "consolidated_slot_storage_utilization", db_name=self.tenant_info["alteryx_out_db_name"], tag_columns=['rack_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'],retention_policy =rp_seven_days)
        return None
    


with DAG(
    'slot_storage_utilization',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'slot_storage_utilization',
    schedule_interval = timedelta(hours=2),
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['slot_storage_utilization'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                final_task = PythonOperator(
                    task_id='slot_storage_utilization_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(slot_storage_utilization().slot_storage_utilization_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=7200),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='slot_storage_utilization_final',
            provide_context=True,
            python_callable=functools.partial(slot_storage_utilization().slot_storage_utilization_final,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )
