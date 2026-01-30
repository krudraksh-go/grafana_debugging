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

class barcode_travelled:
    def barcode_travelled_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        q2 = f"SHOW FIELD KEYS FROM  barcode_travelled "
        df2 = pd.DataFrame(self.client.query(q2).get_points())
        butler_idtype= "integer"
        if not df2.empty:
            butler_idtype= df2[df2['fieldKey']=='butler_id']
            if not butler_idtype.empty:
                butler_idtype=butler_idtype.reset_index()
                butler_idtype=butler_idtype['fieldType'][0]

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        try:
            daterange = self.client.get_datetime_interval2("barcode_travelled", '1d',self.tenant_info['StartTime'])
            for i in daterange.index:
                    self.start_date = daterange['start_date'][i]
                    self.end_date = daterange['end_date'][i]
                    self.barcode_travelled_final1(butler_idtype, **kwargs)
        except AirflowTaskTimeout as timeout_exception:
            raise timeout_exception                    
        except Exception as e:
            print(f"error:{e}")
            raise e

    def barcode_travelled_final1(self,butler_idtype, **kwargs):
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        databases = self.read_client.get_list_database()
        gmc_db = self.tenant_info["out_gmc_db_name"]
        gmr_db = self.tenant_info["out_db_name"]
        database_exists = any(db['name'] == gmc_db for db in databases)
        if database_exists:
            q = f"select count(barcode) as barcode from {gmc_db}..butler_step_logs  where time>='{self.start_date}' and time <'{self.end_date}' group by host,installation_id order by time desc"
        else:
            q = f"select count(barcode) as barcode from {gmr_db}..butler_step_logs  where time>='{self.start_date}' and time <'{self.end_date}' group by host,installation_id order by time desc"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if not df.empty:
            if butler_idtype=="integer":
                df['butler_id'] = 1
            else:
                df['butler_id'] = "1"
        else:
            if butler_idtype=="integer":
                butler_id = 1
            else:
                butler_id = "1"

            d={'time':[self.start_date],'barcode':[0],'butler_id':[butler_id]}
            df = pd.DataFrame(d)
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "barcode_travelled", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None


with DAG(
    'Barcode_travelled',
    default_args = {**CommonFunction().get_default_args_for_dag(), **{'retries':3, 'retry_delay':timedelta(minutes=1)}},
    description = 'Barcode Traveelled',
    schedule_interval = '0 7,23 * * *',
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
            if tenant['Active'] == "Y" and tenant['Barcode_Travelled'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                final_task = PythonOperator(
                    task_id='barcode_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(barcode_travelled().barcode_travelled_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='barcode_travelled_final',
            provide_context=True,
            python_callable=functools.partial(barcode_travelled().barcode_travelled_final,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

