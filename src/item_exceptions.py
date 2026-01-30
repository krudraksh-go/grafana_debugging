## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, Elasticsearch
import numpy as np
import requests
import json
from config import (
    rp_seven_days , MongoDbServer
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

class ItemExceptions:

    def get_payload(self, start_date, end_date):

        payload = json.dumps({
        "size": 5000,
        "from": 0,
        "sort": [
            {
            "updatedOn": {
                "order": "asc"
            }
            }
        ],
        "query": {
            "bool": {
            "filter": {
                "bool": {
                "must": [{
                    "range": {
                        "updatedOn": {
                        "gte": start_date,
                        "lte": end_date
                        }
                    }
                    },
                    {
                    "range": {
                        "sr_products_counts.exceptions": {
                        "gte": 1
                        }
                    }
                    },
                    {
                    "bool": {
                        "should": [
                        {
                            "bool": {
                            "must_not": {
                                "exists": {
                                "field": "fulfillmentArea"
                                }
                            }
                            }
                        },
                        {
                            "terms": {
                            "fulfillmentArea.keyword": [
                                "gtp",
                                "assist_area"
                            ]
                            }
                        }
                        ]
                    }
                    }
                ]
                }
            }
            }
        }
        })
                
        hits = self.elastic_client.fetch_doc_with_payload('servicerequest',payload, batch_size=5000)
        df = pd.json_normalize([hit["_source"] for hit in hits])
        return df

    def get_pps_id(self, x):
        result = 'PPS '
        for i in range(len(x)):
            result = result + str(x[i])+ ' '
        return result

    def get_product_attribute(self, x):
        if 'products' in x and len(x['products'])>0:
            res = x['products'][0]
            if 'productAttributes' in res:
                return res['productAttributes']
        return {}
    
    def get_product_quantity(self, x):
        if 'products' in x and len(x['products'])>0:
            res = x['products'][0]
            if 'productQuantity' in res:
                return res['productQuantity']
        return '--'
    
    def get_sku_dimension(self, x):
        if 'products' in x and len(x['products'])>0:
            res = x['products'][0]
            if 'productAttributes' in res and res['productAttributes']!=None and 'sku_dimension' in res['productAttributes']:
                res =  res['productAttributes']['sku_dimension']
                unit = 'cm'
                if 'uom' in res:
                    unit = res['uom']
                result = ''
                for dimension in ['length','breadth','height']:
                    if dimension in res:
                        result = result + res[dimension]+ ' ' + unit + ' * '
                return result[:-2]
        return '--'    

    def item_exception(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")
        self.elastic_client = Elasticsearch(host=self.tenant_info["elastic_ip"], port=self.tenant_info["elastic_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info["Name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date(f"{rp_seven_days}.pick_put_item_exceptions", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_exceptions where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        item_exceptions = pd.DataFrame(self.read_client.query(q).get_points())
        q = f"select *  from put_exception_events where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        put_exception_events = pd.DataFrame(self.read_client.query(q).get_points())     

        if item_exceptions.empty and put_exception_events.empty:
            print("No data found in item_exceptions and put_exception_events")
            self.end_date = datetime.now(timezone.utc)
            self.item_exception1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3(f"{rp_seven_days}.pick_put_item_exceptions", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.item_exception1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def item_exception1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date(f"{rp_seven_days}.pick_put_item_exceptions", self.tenant_info)
        self.start_date = pd.to_datetime(self.start_date).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(self.start_date, self.end_date)

        payload = self.get_payload(self.start_date, self.end_date)
        if payload.empty:
            print("No data found in elastic search")
            filter = {"site": self.tenant_info['Name'], "table": f"{rp_seven_days}.pick_put_item_exceptions"}
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter, new_last_run)
            return None 
        cols_to_create = ['Exception Type','time','Operator','Pps_Seat_Name','source','destination','Container Id','uom','sku','quantity','Sku Dimensions','Slot Dimensions','Reason']
        for col in cols_to_create:
            payload[col] = '--'
        payload = payload.rename(columns = {'externalServiceRequestId':'order_id','type':'Order Type','exceptions.containers':'container','attributes.pps_id':'pps_id'})
        
        df = payload[payload['container'].apply(lambda x: len(x) > 0)].reset_index(drop=True)
        if df.empty:
            print("No data found in elastic search with valid exception count")
            filter = {"site": self.tenant_info['Name'], "table": f"{rp_seven_days}.pick_put_item_exceptions"}
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter, new_last_run)
            return None 
        df['container'] = df['container'].apply(lambda x: x[0])
        df['pps_id'] = df['pps_id'].apply(lambda x: self.get_pps_id(x))
        df['Exception Type'] = df['container'].apply(lambda x: x['state'] if 'state' in x else '--')
        df['time'] = df['container'].apply(lambda x: x['updatedOn'] if 'updatedOn' in x else '--')
        df['containerAttributes'] = df['container'].apply(lambda x: x['containerAttributes'] if 'containerAttributes' in x and pd.isna(x) else {})
        df['Operator'] = df['containerAttributes'].apply(lambda x: x['user_name'] if 'user_name' in x and pd.isna(x) else '--')
        df['Pps_Seat_Name'] = df['containerAttributes'].apply(lambda x: x['pps_seat_name'] if 'pps_seat_name' in x and pd.isna(x) else '--')
        df['source'] = df['containerAttributes'].apply(lambda x: x['location'] if 'location' in x and pd.isna(x) else '--')
        df['destination'] = df[['Exception Type','pps_id']].apply(lambda x: x['pps_id'] if x['Exception Type']!='missing' else '--', axis=1)
        if 'pps_id' in df.columns:
            del df['pps_id']
        df['Container Id'] = df['container'].apply(lambda x: x['barcode'] if 'barcode' in x else '--')
        df['product_attribute'] = df['container'].apply(lambda x: self.get_product_attribute(x))
        df['product_attribute'] = df['product_attribute'].apply(lambda x : {'package_name': '--', 'pdfa_values': {'product_sku': '--'}} if pd.isna(x) or x==None else x)
        df['uom'] = df['product_attribute'].apply(lambda x: x['package_name'] if 'package_name' in x else '--')
        df['sku'] = df['product_attribute'].apply(lambda x: x['pdfa_values']['product_sku'] if 'pdfa_values' in x and 'product_sku' in x['pdfa_values'] else '--')
        df['quantity'] = df['container'].apply(lambda x: self.get_product_quantity(x))
        df['Sku Dimensions'] = df['container'].apply(lambda x: self.get_sku_dimension(x))
        df['Slot Dimensions'] = df['containerAttributes'].apply(lambda x:x['container_dimension'] if 'container_dimension' in x and pd.isna(x) else '--')
        df['Reason'] = df['containerAttributes'].apply(lambda x:x['reason'] if 'reason' in x and pd.isna(x) else '--')
        cols = ['order_id','Order Type','Exception Type','source','destination','Operator','time','Pps_Seat_Name','Container Id','uom','quantity','sku','Sku Dimensions','Slot Dimensions','Reason','createdOn']        
        df = df[cols]
        df = df.fillna('--')
        df['time'] = pd.to_datetime(df['time'].str.slice(0, 19), format='%Y-%m-%dT%H:%M:%S')
        df['host'] = self.tenant_info['host_name']
        df['installation_id'] = self.tenant_info['installation_id']
        df['host'] = df['host'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)

        df = df.set_index('time')
        for col in df.columns:
            df[col] = df[col].astype(str)
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "pick_put_item_exceptions", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['order_id'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy=rp_seven_days)
        print("inserted")
        return None


with DAG(
        'Pick_put_item_exceptions',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Exceptions raised during pick put item operations',
        schedule_interval='29 * * * *',
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
            if tenant['Active'] == "Y" and tenant['pick_put_item_exceptions'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='item_exception_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(ItemExceptions().item_exception,
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
            task_id='item_exception_final',
            provide_context=True,
            python_callable=functools.partial(ItemExceptions().item_exception,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )


