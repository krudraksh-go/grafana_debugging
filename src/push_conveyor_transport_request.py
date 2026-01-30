## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import time
import pandas as pd
import kafka
from kafka import KafkaConsumer
import json
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, MongoDBManager
import os
from push_conveyor_container_recieve_to_mongodb import ConveyorContainerRecieveToMongoDB
from push_conveyor_container_send_events import ConveyorContainerSendEventsToMongoDB
from config import (
    GOOGLE_AUTH_JSON,
    CONFFILES_DIR,
    GOOGLE_BUCKET_NAME,
    MongoDbServer,
    rp_seven_days
)

class CionveyorTransportRequestToMongoDB:

    def is_valid_data(self, data):
        if 'source_service' in data and data['source_service'] in ['GMC','gmc']:
            return True
        else:
            return False

    def inserting_data_to_mongodb(self, data):
        if not data:
            print("No data to insert.")
            return
        buffer = pd.DataFrame(data)
        buffer['timestamp'] = buffer['timestamp'].apply(lambda x: self.CommonFunction.parse_timestamp(x))
        print(f"Inserting {len(data)} messages into DB...")
        json_list = buffer.to_json(orient="records")
        json_list = json.loads(json_list)
        self.cls_mdb = MongoDBManager(self.connection_string, self.database_name, self.collection_name)
        insert = self.cls_mdb.insert_data(json_list) 

    def push_data_to_mongodb(self, tenant_info, **kwargs):
        tenant_info = tenant_info['tenant_info']
        bootstrap_servers = tenant_info['bootstrap_servers']
        self.is_production = tenant_info['is_production']
        site_name = tenant_info['Name']
        self.CommonFunction = CommonFunction()
        admin_client = kafka.KafkaAdminClient(bootstrap_servers=[bootstrap_servers])
        all_topics = admin_client.list_topics()
        topics = [item for item in all_topics if item.endswith('conveyor-transport-request.requests')]


        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=site_name+'conveyor-transport-request-requests',
            auto_offset_reset='latest'
        )
        self.connection_string = MongoDbServer
        self.database_name = "kafka_data"
        self.collection_name = site_name + "conveyor-transport-request-requests"

        data = []
        last_flush_time = time.time()
        FLUSH_INTERVAL = 10  # seconds
        start_time = time.time()

        print("Consuming and batching...")

        try:
            for message in consumer:
                d = json.loads(message.value.decode('utf-8'))
                if self.is_production == 'Y':
                    d['installation_id'] = 'butler_demo'
                else:
                    d['installation_id'] = message.topic.split('.')[0]                
                if self.is_valid_data(d):
                    data.append(d)  
                now = time.time()
                if (now - last_flush_time) >= FLUSH_INTERVAL:
                    self.inserting_data_to_mongodb(data)
                    data.clear()
                    last_flush_time = now
                if (now - start_time) >= 60 * 10:
                    if data:
                        self.inserting_data_to_mongodb(data)
                        data.clear()                    
                    print("Stopping consumer after 10 minutes...")
                    break
            print("out")
        except Exception as e:
            print("Stopping...")
            print(f"Error consuming messages: {e}")
            raise e
        finally:
            print("finnaly")
            if data:
                self.inserting_data_to_mongodb(data)
            consumer.close()
            self.cls_mdb.close_connection()

with DAG(
        'Conveyor_transport_request_to_mongodb',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Read Data from conveyor-transport-request.requests Kafka topic and push to MongoDB',
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
            if tenant['Active'] == "Y" and tenant['conveyor_communication_delay_analysis'] == "Y":
                try:
                    final_task = PythonOperator(
                        task_id='conveyor_transport_request_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(CionveyorTransportRequestToMongoDB().push_data_to_mongodb,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=1200),
                    )
                    Client_setting_final_task = PythonOperator(
                        task_id='conveyor_container_recieve_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(ConveyorContainerRecieveToMongoDB().push_data_to_mongodb,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=1200), )

                    Client_setting_final_task2 = PythonOperator(
                        task_id='conveyor_container_send_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(ConveyorContainerSendEventsToMongoDB().push_data_to_mongodb,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=1200), )   
                
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='conveyor_transport_request_final',
            provide_context=True,
            python_callable=functools.partial(CionveyorTransportRequestToMongoDB().push_data_to_mongodb,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

