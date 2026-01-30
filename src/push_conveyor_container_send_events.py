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
from config import (
    GOOGLE_AUTH_JSON,
    CONFFILES_DIR,
    GOOGLE_BUCKET_NAME,
    MongoDbServer,
    rp_seven_days
)

class ConveyorContainerSendEventsToMongoDB:

    def is_valid_data(self, data):
        if 'data' in data and 'originating_service' in  data['data'] and data['data']['originating_service'] in ['GMC','gmc']:
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
        topics = [item for item in all_topics if item.endswith('conveyor-container-send.events')]

        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=site_name+'conveyor_container_send_events',
            auto_offset_reset='latest'
        )
        self.connection_string = MongoDbServer
        self.database_name = "kafka_data"
        self.collection_name = site_name + "conveyor-container-send-events"

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
