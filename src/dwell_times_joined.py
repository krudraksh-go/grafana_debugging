## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------
import functools

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
import pandas as pd
from influxdb import InfluxDBClient

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '172.28.28.10')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '172.28.28.10')
db_name =os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------


class DwellTimeError:
    def time_format_convert(self, df_time):
        df_time['last_time'] = pd.to_datetime(df_time['time'])
        time = df_time['last_time'][0].to_pydatetime()
        dt = datetime.fromisoformat(str(time))
        time_int = dt.timestamp()
        timestamp_unix = int(time_int * 1e9)
        str_time = str(timestamp_unix)
        return str_time

    def get_joined_table_last_time(self):
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")


        #client = InfluxDBClient(host=influx_ip,  port=influx_port, database='GreyOrange')
        query = f"SELECT last(date) FROM dwell_times_joined"
        d = self.read_client.query(query)
        time= list(d.get_points())  # list of dict last row
        if len(time)==0:
            print("JOINED TABLE NOT CREATED YET")
            return 0
        else:
            unix_time= self.time_format_convert(pd.DataFrame(time))
            return unix_time


    def joining_influx_tables(self,threshold_time):
#        client = InfluxDBClient(host=influx_ip,  port=influx_port, database='GreyOrange')
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"], db="GreyOrange")


        measurement1 = 'actual_dwell_times'
        if threshold_time == 0:
            query1 = f"SELECT * FROM actual_dwell_times"
        else:
            query1 = f"SELECT * FROM actual_dwell_times where time > {threshold_time}"

        result1 = self.read_client.query(query1)
        data1 = list(result1.get_points(measurement=measurement1))
        df1 = pd.DataFrame(data1)
        measurement2 = 'pred_dwell_times'

        if not df1.empty:
            query2 = f"SELECT estimated_dwell_times,is_default,task_id FROM pred_dwell_times WHERE time > now() - 30m"
            result2 = self.read_client.query(query2)
            data2 = list(result2.get_points(measurement=measurement2))
            df2 = pd.DataFrame(data2)
            joined_df = pd.merge(df1, df2, on='task_id')
        else:
            joined_df = pd.DataFrame()
            print("ACTUAL DATA IS NOT FILLED UP YET")

        return joined_df

    def post_processing(self,joined_data):
        # joined_data = joined_data.compute()
        joined_data['date'] = joined_data['time_x']
        joined_data['time'] = pd.to_datetime(joined_data['time_x'])
        joined_data= joined_data.set_index('time')  #this is important
        joined_data = joined_data.drop(columns=['time_x','time_y'])
        joined_data['dwell_time_error'] = joined_data['estimated_dwell_times'] - joined_data['actual_dwell_time']
        # joined_data.set_index('unix',inplace=True)
        return joined_data


    def write_data(self,dataframe):
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(dataframe, "dwell_times_joined", db_name=self.tenant_info["out_db_name"],
                                      tag_columns=['is_default','task_type','installation_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

#        from influxdb import DataFrameClient
#        client = DataFrameClient(host=influx_ip, port=influx_port, database= "GreyOrange")
#         client.write_points(dataframe, database='GreyOrange'
#                             , protocol='line',
#                             measurement="dwell_times_joined",
#                             tag_columns= ['is_default','task_type','installation_id'])

    def function_calls(self,tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        last_timestamp = self.get_joined_table_last_time()
        joined_table = self.joining_influx_tables(last_timestamp)
        if not joined_table.empty:
            post_processing =self.post_processing(joined_table)
            self.write_data(post_processing)
            print(joined_table)
            print("length of joined data is ", len(joined_table))
        else:
            print("========= NO NEW DATA FORMED YET ============")

with DAG(
        'Dwell_time_error',
        default_args = CommonFunction().get_default_args_for_dag(),
        description = 'Dwell time error logging for OBTS  GM-92753',
        schedule_interval = '10 * * * *',
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
            if tenant['Active'] == "Y" and tenant['DwellTimeError'] == "Y":
                final_task = PythonOperator(
                    task_id='DwellTimeError_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(DwellTimeError().function_calls,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='DwellTimeError',
            provide_context=True,
            python_callable=functools.partial(DwellTimeError().function_calls,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )



