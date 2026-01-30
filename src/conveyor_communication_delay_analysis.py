import kafka
from kafka import KafkaConsumer, TopicPartition
import time
import json
import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, MongoDBManager
import os
from config import (
    CONFFILES_DIR,
    GOOGLE_BUCKET_NAME,
    MongoDbServer
    )

class ConveyorCommunicationDelayAnalysis:
    
    def conveyor_communication_delay_analysis(self,tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.CommonFunction = CommonFunction()
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

        check_start_date = self.client.get_start_date("conveyor_communication_delay_analysis", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = int(check_start_date.timestamp())
        check_end_date = int(pd.to_datetime(check_end_date).timestamp())
        print(f"check start date: {check_start_date} check end date: {check_end_date}")

        q = {
            'timestamp': { '$gt': check_start_date, '$lt': check_end_date } 
            }
        connection_string = MongoDbServer
        database_name = "kafka_data"
        collection_name = self.site + "conveyor-transport-request-requests"
        self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
        cursor = self.cls_mdb.collection.find(q).limit(1)
        data= list(cursor)
        print(len(data))
        if(len(data) == 0):
            self.end_date = datetime.now(timezone.utc)
            self.conveyor_communication_delay_analysis1(self.end_date, **kwargs)        
        else:
            try:
                daterange = self.client.get_datetime_interval3("conveyor_communication_delay_analysis", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.conveyor_communication_delay_analysis1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e    
    

    def conveyor_communication_delay_analysis1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("conveyor_communication_delay_analysis", self.tenant_info)
        self.start_date = pd.to_datetime(self.start_date) - timedelta(minutes=30)
        self.start_date = self.start_date.strftime("%Y-%m-%d %H:%M:%S")
        self.end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
        print(f"start date: {self.start_date}, end date: {self.end_date}")

        self.connection_string = MongoDbServer
        self.database_name = "kafka_data"

        self.cls_mdb_conveyor_container_send = MongoDBManager(self.connection_string, self.database_name, self.site + "conveyor_container_recieve_requests")
        self.cls_mdb_conveyor_container_receive = MongoDBManager(self.connection_string, self.database_name,self.site + "conveyor_container_recieve_requests")
        self.cls_mdb_conveyor_transport_request = MongoDBManager(self.connection_string, self.database_name, self.site + "conveyor-transport-request-requests")

        q = {
            'timestamp': { '$gt': self.start_date, '$lt': self.end_date } }
        
        conveyor_container_send_data = self.cls_mdb_conveyor_container_send.get_data(q)
        conveyor_container_receive_data = self.cls_mdb_conveyor_container_receive.get_data(q)
        conveyor_transport_request_data = self.cls_mdb_conveyor_transport_request.get_data(q)

        conveyor_container_send_df = pd.DataFrame(conveyor_container_send_data)
        conveyor_container_receive_df = pd.DataFrame(conveyor_container_receive_data)
        conveyor_transport_request_df = pd.DataFrame(conveyor_transport_request_data)
        

        conveyor_container_receive_df = conveyor_container_receive_df.rename(columns={'container_id':'tote_id','timestamp':'time','msg_type':'event'})
        conveyor_container_receive_df = conveyor_container_receive_df[['time','event','conveyor_id','tote_id','installation_id']]
        conveyor_container_receive_df = conveyor_container_receive_df.fillna('')

        conveyor_container_send_df = conveyor_container_send_df.rename(columns={'container_id':'tote_id','timestamp':'time','msg_type':'event'})
        conveyor_container_send_df = conveyor_container_send_df[['time','event','conveyor_id','tote_id','installation_id']]

        conveyor_transport_request_df2 = pd.json_normalize(conveyor_transport_request_df['payload'])[['tote_id','task_details.task_type']]
        conveyor_transport_request_df =  pd.concat([conveyor_transport_request_df,conveyor_transport_request_df2],axis=1).fillna('')
        conveyor_transport_request_df = conveyor_transport_request_df.rename(columns={'container_id':'tote_id','timestamp':'time','task_details.task_type':'event','entity_id':'conveyor_id'})
        conveyor_transport_request_df = conveyor_transport_request_df[['time','event','conveyor_id','tote_id','installation_id']]  

        df = pd.concat([conveyor_container_receive_df,conveyor_container_send_df,conveyor_transport_request_df])

        df = df.sort_values(by=['installation_id','conveyor_id', 'time'], ascending=[True,True, True])
        df['time'] = pd.to_datetime(df['time'])
        df['prev_event'] = df['event'].shift(1)
        df['next_event'] = df['event'].shift(-1)
        df['prev_time'] = df['time'].shift(1)
        df['next_time'] = df['time'].shift(-1)
        df['prev_conveyor_id'] = df['conveyor_id'].shift(1)
        df['next_conveyor_id'] = df['conveyor_id'].shift(-1)
        df['prev_installation_id'] = df['installation_id'].shift(1)
        df['next_installation_id'] = df['installation_id'].shift(-1)

        df['time_diff_with_prev_event'] = np.nan
        df['time_diff_with_next_event'] = np.nan      
        df['time_diff_with_prev_event'] = df.apply(lambda x: ((x['time'] - x['prev_time']).total_seconds() * 1000) if x['conveyor_id']==x['prev_conveyor_id'] and x['installation_id']==x['prev_installation_id'] else np.nan, axis=1)
        df['time_diff_with_next_event'] = df.apply(lambda x: ((x['next_time'] - x['time']).total_seconds() * 1000) if x['conveyor_id']==x['next_conveyor_id'] and x['installation_id']==x['next_installation_id'] else np.nan, axis=1)
        
        df = df[(~pd.isna(df['time_diff_with_next_event'])) & (~pd.isna(df['time_diff_with_prev_event']))]   

        notified_conveyor_events = ['conveyor_entry_to_pick','track_to_exit','pick_to_exit']
        df = df[df['event'].isin(notified_conveyor_events)].reset_index(drop=True)    

        df = df[['time','conveyor_id','event','prev_event','next_event','time_diff_with_prev_event','time_diff_with_next_event','installation_id']]
        str_cols = ['installation_id','conveyor_id','event','prev_event','next_event']
        float_cols = ['time_diff_with_prev_event','time_diff_with_next_event']
        df[float_cols] = df[float_cols].fillna(0)
        df = self.CommonFunction.str_typecast(df,str_cols)
        df = self.CommonFunction.float_typecast(df,float_cols)
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')          

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "conveyor_communication_delay_analysis", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['conveyor_id'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])                 


with DAG(
        'Conveyor_communication_delay_analysis',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Time taken by conveyor to communicate',
        schedule_interval='53 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['conveyor_communication_delay_analysis'] == "Y":
                Operator_working_time_final_task = PythonOperator(
                    task_id='Conveyor_communication_delay_analysis_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(ConveyorCommunicationDelayAnalysis().conveyor_communication_delay_analysis,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:

        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='Conveyor_communication_delay_analysis_final',
            provide_context=True,
            python_callable=functools.partial(ConveyorCommunicationDelayAnalysis().conveyor_communication_delay_analysis,
                                              tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )