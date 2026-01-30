## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, Butler_api
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name =os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class rack_volume_details:
    def rack_volume_details_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.utilfunction = CommonFunction()
        self.butler_interface = Butler_api(host=self.tenant_info['Butler_ip'], port=self.tenant_info['Butler_interface_port'])
        self.butler_interface.fetch_rack_storage_utilization()
       # rack_api = "http://192.168.32.86:30408/rack_storage_utilization/_search?size=100"
       # rack_info_req = requests.get(rack_api)
        rack_data = self.butler_interface.rack_storage_utilization_data
        rack_data = rack_data['hits']
        rack_data = rack_data['hits']
        df_nested_list = pd.json_normalize(rack_data)
        df_nested_list = df_nested_list.drop(columns=['_index', '_type', '_id', '_score'])
        df_nested_list = df_nested_list[
            ['_source.cg_breached', '_source.free_volume', '_source.num_of_slots', '_source.rack_id',
             '_source.rack_type', '_source.rack_utilization', '_source.rack_volume', '_source.rack_weight',
             '_source.unique_sku_count', '_source.utilized_volume']]

        df_nested_list.rename(columns={'_source.cg_breached': 'cg_breached', '_source.free_volume': 'free_volume',
                                       '_source.num_of_slots': 'num_of_slots', '_source.rack_id': 'rack_id',
                                       '_source.rack_volume': 'rack_volume',
                                       '_source.rack_type': 'rack_type', '_source.rack_utilization': 'rack_utilization',
                                       '_source.rack_weight': 'rack_weight',
                                       '_source.unique_sku_count': 'unique_sku_count',
                                       '_source.utilized_volume': 'utilized_volume'
                                       }, inplace=True)

        df_nested_list['MSU_regularity'] = df_nested_list['rack_type'].apply(
            lambda x: 'Regular_MSU' if 'CLICK_COLLECT_2' in x.upper() else 'Irregular_MSU')
        cron_run_at = datetime.now()
        df_nested_list['cron_ran_at'] = cron_run_at.strftime('%Y-%m-%dT%H:%M:%S')
        df_nested_list['cron_ran_at']= df_nested_list['cron_ran_at'].apply(str)
        df_nested_list['cg_breached'] = df_nested_list['cg_breached'].apply(lambda x:1 if x == "True" else 0)
        df_nested_list['cg_breached'] = df_nested_list['cg_breached'].astype(float)
        df_nested_list['free_volume'] = df_nested_list['free_volume'].astype(float)
        df_nested_list['num_of_slots'] = df_nested_list['num_of_slots'].astype(float)
        df_nested_list['rack_utilization'] = df_nested_list['rack_utilization'].astype(float)
        df_nested_list['rack_volume'] = df_nested_list['rack_volume'].astype(float)
        df_nested_list['rack_weight'] = df_nested_list['rack_weight'].astype(float)
        df_nested_list['unique_sku_count'] = df_nested_list['unique_sku_count'].astype(float)
        df_nested_list['utilized_volume'] = df_nested_list['utilized_volume'].astype(float)
        df_nested_list = self.utilfunction.reset_index(df_nested_list)
        df_nested_list = self.utilfunction.get_epoch_time(df_nested_list, date1=cron_run_at)
        df_nested_list['time'] = df_nested_list.time_ns
        df_nested_list.time = pd.to_datetime(df_nested_list.time)
        df_nested_list = df_nested_list.drop(columns=['time_ns', 'index'])
        df_nested_list['host'] = self.tenant_info['host_name']
        df_nested_list['installation_id'] = self.tenant_info['installation_id']
        df_nested_list['host'] = df_nested_list['host'].astype(str)
        df_nested_list['installation_id'] = df_nested_list['installation_id'].astype(str)
        df_nested_list = df_nested_list.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df_nested_list, "rack_volume_details", db_name=self.tenant_info["alteryx_out_db_name"],
                                      tag_columns=['MSU_regularity', 'rack_id', 'rack_type'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None


with DAG(
    'rack_volume_details',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'rack_volume_details',
    schedule_interval = '@daily',
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
            if tenant['Active'] == "Y" and tenant['rack_volume_details'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                final_task = PythonOperator(
                    task_id='barcode_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(rack_volume_details().rack_volume_details_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='rack_volume_details_final',
            provide_context=True,
            python_callable=functools.partial(rack_volume_details().rack_volume_details_final,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

