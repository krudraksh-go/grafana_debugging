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
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, MongoDBManager
from config import (
    CONFFILES_DIR,
    GOOGLE_BUCKET_NAME,
    MongoDbServer
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

class ConveyorThroughputAnalysis:


    def conveyor_throughput_analysis(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("conveyor_throughput_analysis", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = {
            'timestamp': { '$gt': check_start_date, '$lt': check_end_date },
            'msg_type': { '$in': ['container_reached', 'container_dropped_ack', 'container_lifted_ack'] }
        }
        connection_string = MongoDbServer
        database_name = "kafka_data"
        collection_name = self.site + "conveyor-container-send-events"
        self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
        cursor = self.cls_mdb.collection.find(q).limit(1)
        data= list(cursor)
        print(len(data))
        if(len(data) == 0):
            self.end_date = datetime.now(timezone.utc)
            self.conveyor_throughput_analysis1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("conveyor_throughput_analysis", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("conveyor_throughput_analysis", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.conveyor_throughput_analysis1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e


    def conveyor_throughput_analysis1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("conveyor_throughput_analysis", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')


        q = {
            'timestamp': { '$gt': self.start_date, '$lt': self.end_date },
            'msg_type': { '$in': ['container_reached', 'container_dropped_ack', 'container_lifted_ack'] }
        }
        
        data = self.cls_mdb.get_data(q)
        if not data:
            filter = {"site": self.tenant_info['Name'], "table": "conveyor_throughput_analysis"}
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter, new_last_run)            
            
        else:
            df = pd.DataFrame(data)
            df = pd.concat([df,pd.json_normalize(df['data'])], axis=1)
            df = df[(df['originating_service']=='gmc') | (df['originating_service']=='GMC')][['timestamp','conveyor_id','msg_type','container_id']].reset_index(drop=True)
            df = df[(df['msg_type'] == 'container_reached') | (df['msg_type'] == 'container_dropped_ack') | (df['msg_type'] == 'container_lifted_ack')].reset_index(drop=True)

            df.rename(columns={'timestamp':'time', 'msg_type':'event', 'container_id':'tote_id'},inplace=True)
            df['time'] = pd.to_datetime(df['time'])
            df = df.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df, "conveyor_throughput_analysis", db_name=self.tenant_info["alteryx_out_db_name"],
                                          tag_columns=['conveyor_id'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None


with DAG(
        'Conveyor_Throughput_Analysis',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Calcualtion of Conveyor Throughput',
        schedule_interval='*/15 * * * *',
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
            if tenant['Active'] == "Y" and tenant['conveyor_throughput_analysis'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='conveyor_throughput_ananlysis_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(ConveyorThroughputAnalysis().conveyor_throughput_analysis,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=600),
                    )
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
            task_id='conveyor_throughput_ananlysis_final',
            provide_context=True,
            python_callable=functools.partial(ConveyorThroughputAnalysis().conveyor_throughput_analysis,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

