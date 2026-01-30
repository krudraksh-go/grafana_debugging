## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
from pandasql import sqldf
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')


# dag = DAG(
#     'cell_voltages',
#     default_args = default_args,
#     description = 'calculation of cell_voltages GM-44036',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class cell_voltages:
    def ideal_dataset(self):
        ideal_dataset = pd.DataFrame(columns=['time','butler_id','max_cell_voltage','min_cell_voltage','value'])
        ideal_dataset['butler_id'] =1
        ideal_dataset['max_cell_voltage'] = "0"
        ideal_dataset['min_cell_voltage'] = "0"
        ideal_dataset['value'] = "0"
        ideal_dataset['time'] = self.end_date
        return ideal_dataset
    def cell_voltages(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("cell_voltages_ayx", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from cell_voltages_delta_alert where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.cell_voltages1(self.end_date, **kwargs)
        else:
            daterange = self.client.get_datetime_interval3("cell_voltages_ayx", '8h', self.tenant_info)
            try:
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.cell_voltages1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
        
    def cell_voltages1(self, end_date, **kwargs):
        self.all_errors = {'Error Message': [], 'Query': []}
        self.start_date = self.client.get_start_date("cell_voltages_ayx", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        #self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.read_influxclient = self.read_client.influx_client
        q=f"select * from cell_voltages_delta_alert  where  time >= '{self.start_date}' and time < '{self.end_date}' order by time desc"
        
        q2="SHOW FIELD KEYS FROM  cell_voltages_ayx "
        df2 = pd.DataFrame(self.client.query(q2).get_points())
        power_level_type= "float"
        if not df2.empty:
            power_level_type= df2[df2['fieldKey']=='power_level']
            if not power_level_type.empty:
                power_level_type=power_level_type.reset_index()
                if power_level_type.shape[0]>1:
                    power_level_type=""
                else:
                    power_level_type=power_level_type['fieldType'][0]
            else:
                power_level_type = "float"

        
        
        data_dict = self.read_client.fetch_data(self.read_influxclient, q)
        error_data = {'Error Message': [], 'Query': []}
        if 'error' in data_dict[0].keys():
            error_data['Error Message'].append(data_dict[0]['error'])
            error_data['Query'].append(data_dict[0]['query'])
            self.all_errors['Error Message'].append(data_dict[0]['error'])
            self.all_errors['Query'].append(data_dict[0]['query'])
        else:
            d = pd.DataFrame(data_dict)

        if ('Please check the query' in error_data['Error Message'] or 'No data fetched' in error_data[
            'Error Message']):
            d= self.ideal_dataset()
        else:
            d['max_cell_voltage'] = d['max_cell_voltage'].astype(float)
            d['min_cell_voltage'] = d['min_cell_voltage'].astype(float)
            d['value'] = d['max_cell_voltage'] - d['min_cell_voltage']
            if 'is_charging' in d.columns:
                d.drop(['is_charging'], axis=1, inplace=True)
            d['max_cell_voltage'] = d['max_cell_voltage'].astype('str')
            d['min_cell_voltage'] = d['min_cell_voltage'].astype('str')
            d['butler_id'] = d['butler_id'].astype('str')
            d['installation_id'] = d['installation_id'].astype('str')
            d['host'] = d['host'].astype('str')
        
        if 'power_level' in d.columns:
            if power_level_type =='float':
                d['power_level'] = d['power_level'].astype(float)
            elif power_level_type =='string':
                d['power_level'] = d['power_level'].astype(str)
            else:
                d = d.drop(columns=['power_level'])
        if 'cell_imbalance_lower_limit' in d.columns:
            d['cell_imbalance_lower_limit'] = d['cell_imbalance_lower_limit'].astype(float)
            d['cell_imbalance_upper_limit'] = d['cell_imbalance_upper_limit'].astype(float)
        if 'difference' in d.columns:
            d['difference'] = d['difference'].astype(float)
        if 'moving_average' in d.columns:
            d['moving_average'] = d['moving_average'].astype(float)            
        if 'power_level_to_consider_cell_voltage_data' in d.columns:
            power_level_type = "float"
            if not df2.empty:
                power_level_type = df2[df2['fieldKey'] == 'power_level_to_consider_cell_voltage_data']
                if not power_level_type.empty:
                    power_level_type = power_level_type.reset_index()
                    if power_level_type.shape[0] > 1:
                        power_level_type = ""
                    else:
                        power_level_type = power_level_type['fieldType'][0]
                else:
                    power_level_type = "float"
            if power_level_type == 'float':
                d['power_level_to_consider_cell_voltage_data'] = d['power_level_to_consider_cell_voltage_data'].astype(float)
            elif power_level_type == 'integer':
                d['power_level_to_consider_cell_voltage_data'] = d['power_level_to_consider_cell_voltage_data'].astype(int)
            elif power_level_type == 'string':
                d['power_level_to_consider_cell_voltage_data'] = d['power_level_to_consider_cell_voltage_data'].astype(str)
            else:
                d = d.drop(columns=['power_level_to_consider_cell_voltage_data'])

        d.time = pd.to_datetime(d.time)
        d = d.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(d, "cell_voltages_ayx", db_name=self.tenant_info["out_db_name"],
                                      tag_columns=['butler_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'cell_voltages',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of cell_voltages',
    schedule_interval = timedelta(hours=8),
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
            if tenant['Active'] == "Y" and tenant['cell_voltages'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                cell_voltages_final_task = PythonOperator(
                    task_id='cell_voltages_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(cell_voltages().cell_voltages,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        cell_voltages_final_task = PythonOperator(
            task_id='cell_voltages_final',
            provide_context=True,
            python_callable=functools.partial(cell_voltages().cell_voltages,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

