import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData, Write_InfluxData, postgres_connection, CommonFunction, GCS, MongoDBManager
import os
from config import (
    MongoDbServer,
    rp_seven_days
)
import json
class UnassignedTotes:

    def del_columns(self, df, col):
        if col in df.columns:
            del df[col]
        return df
    
    def save_unassign_totes2(self,df):
        def normalize_date(x):
            try:
                if x is None or x == "":
                    return None
                # If epoch (int or float)
                if isinstance(x, (int, float)):
                    # milliseconds vs seconds
                    if x > 1e12:
                        x = x / 1000
                    return datetime.fromtimestamp(x, tz=timezone.utc)
                # If string
                return pd.to_datetime(x, utc=True)
            except Exception:
                return None

        collection_name = self.tenant_info['Name']+'_unassign_totes2'
        self.connect_to_mongodb(collection_name)
        df['time'] = df['time'].apply(lambda x: self.CommonFunction.convert_to_nanosec(x))
        df["last_event_time"] = df["last_event_time"].apply(lambda x: normalize_date(x))
        df['last_event_time'] = df['last_event_time'].apply(lambda x: self.CommonFunction.convert_to_nanosec(x) if not pd.isna(x) and x!='' else x)
        if not df.empty:
            two_days_before = (datetime.now()-timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
            query = {"time":{"$lt":two_days_before}}
            test = self.cls_mdb.delete_data(query, multiple=True)
            json_list = df.to_json(orient="records")
            json_list = json.loads(json_list)
            self.cls_mdb.insert_data(json_list)

    def write_data_to_influx(self, df):
        df['time'] = datetime.now(timezone.utc)
        curr_date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        df['date'] = curr_date
        df = df.set_index(pd.to_datetime(np.arange(len(df)), unit='s', origin=df['date'][0]))
        cols = ['time','date','last_event_name_x','tote_events_time']
        for col in cols:
            df = self.del_columns(df,col)
        df['tote_id'] = df['tote_id'].astype(str, errors='ignore')
        df['last_synced_at'] = df['last_synced_at'].astype(str, errors='ignore')
        df['created_at'] = df['created_at'].astype(str, errors='ignore')
        df['updated_at'] = df['updated_at'].astype(str, errors='ignore')
        df['last_event_name'] = df['last_event_name'].apply(lambda x: 'No Event Found' if pd.isna(x) else x)
        df['sku_id'] = df['sku_id'].apply(lambda x: '' if pd.isna(x) else str(x))
        df['sku_id'] = df['sku_id'].astype(str, errors='ignore')
        df['last_event_name'] = df['last_event_name'].astype(str, errors='ignore')
        df['id'] = df['id'].astype(str, errors='ignore')
        df['tote_count'] = df['tote_count'].astype(float, errors='ignore')
        if 'host' not in df.columns:
            df['host'] = self.tenant_info['host_name']
        else:
            df['host'] = df['host'].apply(lambda x: self.tenant_info['host_name'] if pd.isna(x) else x)

        if 'installation_id' not in df.columns:
            df['installation_id'] = self.tenant_info['installation_id']
        else:
            df['installation_id'] = df['installation_id'].apply(lambda x: self.tenant_info['installation_id'] if pd.isna(x) else x)
        df['host'] = df['host'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)

        df['cron_ran_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if '_id' in df.columns:
            del df['_id']
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                     port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "unassigned_totes", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=[],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'],retention_policy =rp_seven_days)
        
        df = df.reset_index()
        df = df.rename(columns={'index':'time'})
        self.save_unassign_totes2(df[['time','tote_id','last_event_name', "last_event_time"]])
        print("inserted")
        return None

    def connect_to_mongodb(self,collection_name):
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)

    
    def fetch_sku_details(self):
        collection_name = self.tenant_info['Name']+'_sku_details'
        self.connect_to_mongodb(collection_name)
        data = self.cls_mdb.get_data({},{ 'sku_id': 1, 'item_id': 1, '_id': 0 })
        result = pd.DataFrame(data)
        if '_id' in result.columns:
            del result['_id']
        self.cls_mdb.close_connection()
        result.drop_duplicates(subset=['item_id'], keep='first')
        return result

    def fetch_tote_info_from_tower(self):
        cols = ['tote_id', 'status','location_type', 'location','last_synced_at','created_at','updated_at','tote_type','blocked_reason','ranger_id','tray_id','world_coordinate']
        self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                sslrootcert=self.tenant_info['sslrootcert'],
                                                sslcert=self.tenant_info['sslcert'], \
                                                sslkey=self.tenant_info['sslkey'],
                                                host=self.tenant_info['Postgres_tower'], \
                                                port=self.tenant_info['Postgres_tower_port'],
                                                password=self.tenant_info['Tower_password'],
                                                dag_name=os.path.basename(__file__),
                                                site_name=self.tenant_info['Name'])
        qry = f"select tote_id, status,location_type, location,last_synced_at,created_at,updated_at,tote_type,blocked_reason,ranger_id,tray_id,world_coordinate from data_tote where location = 'undefined' or status = 'blocked' "
        # print(qry)
        res = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
        self.postgres_conn.close()
        result = pd.DataFrame(res, columns=cols)
        return result

    def fetch_from_butler_dev(self,tote_ids):
        self.postgres_conn = postgres_connection(database='butler_dev',
                                            user=self.tenant_info['Postgres_butler_user'], \
                                            sslrootcert=self.tenant_info['sslrootcert'],
                                            sslcert=self.tenant_info['sslcert'], \
                                            sslkey=self.tenant_info['sslkey'],
                                            host=self.tenant_info['Postgres_ButlerDev'], \
                                            port=self.tenant_info['Postgres_butler_port'],
                                            password=self.tenant_info['Postgres_butler_password'],
                                            dag_name=os.path.basename(__file__),
                                            site_name=self.tenant_info['Name'])
        qry = f"""SELECT id,left(slot,10) as tote_id, (new_uom::json->>'Item') as "unit_count", transaction_type, old_uom::json->>'Item' as "old_count", item_id
                FROM inventory_transaction_archives where  id in (select max(id) from inventory_transaction_archives where left(slot,10) in ({tote_ids}) group by left(slot,10)) and CAST (new_uom::json->>'Item' as integer) > 0 """
        # print(qry)
        result = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
        self.postgres_conn.close()
        result = pd.DataFrame(result,columns=['id','tote_id','unit_count','transaction_type','old_count','item_id'])
        return result
        
    def fetch_from_butler_dev2(self,items):
        self.postgres_conn = postgres_connection(database='butler_dev',
                                                    user=self.tenant_info['Postgres_butler_user'], \
                                                    sslrootcert=self.tenant_info['sslrootcert'],
                                                    sslcert=self.tenant_info['sslcert'], \
                                                    sslkey=self.tenant_info['sslkey'],
                                                    host=self.tenant_info['Postgres_ButlerDev'], \
                                                    port=self.tenant_info['Postgres_butler_port'],
                                                    password=self.tenant_info['Postgres_butler_password'],
                                                    dag_name=os.path.basename(__file__),
                                                    site_name=self.tenant_info['Name'])
        qry = f"""SELECT item_id,count(distinct left(slot,10))
                FROM inventory_transaction_archives where  id in (select max(id)
                from inventory_transaction_archives
                where item_id in ({items}) group by item_id,slot) and CAST (new_uom::json->>'Item' as integer) > 0
                group by item_id"""
        result = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
        self.postgres_conn.close()
        result = pd.DataFrame(result,columns=['item_id','tote_count'])
        return result
    
    def get_unassign_totes2(self):
        collection_name = self.tenant_info['Name']+'_unassign_totes2'
        self.connect_to_mongodb(collection_name)
        one_days_before = (datetime.now()-timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')
        query = {"time":{"$gt":one_days_before}}
        data = self.cls_mdb.get_data(query,None)
        result = pd.DataFrame(data)
        if '_id' in result.columns:
            del result['_id']
        self.cls_mdb.close_connection()  
        return result

    def final_call(self,tenant_info,**kwargs):
        self.CommonFunction = CommonFunction()
        self.tenant_info = tenant_info['tenant_info']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.influxclient = self.client.influx_client
        unassigned_totes = self.fetch_tote_info_from_tower()
        if unassigned_totes.empty:
            return
        tote_ids = unassigned_totes['tote_id'].to_list()
        totes = "', '".join(tote_ids)
        totes = f"'{totes}'"
        inventory_transaction_archives = self.fetch_from_butler_dev(totes)
        temp = pd.merge(unassigned_totes, inventory_transaction_archives, how = 'left', on = ['tote_id'])
        temp['item_id'] = temp['item_id'].apply(lambda x: '' if pd.isna(x) else str(x))
        items = temp['item_id'].to_list()
        item = "|".join(items)
        item = f"'{item}'"
        item = item.replace("'", "")
        sku_id = self.fetch_sku_details()
        sku_id['item_id'] = sku_id['item_id'].astype(str)
        unassigned_totes = pd.merge(temp, sku_id, how = 'left', on = ['item_id'])
        item_ids = unassigned_totes['item_id'].to_list()
        items = "', '".join(item_ids)
        items = f"'{items}'"
        temp = self.fetch_from_butler_dev2(items)
        df2 = pd.merge(unassigned_totes, temp, how = 'left', on = ['item_id'])

        df = self.get_unassign_totes2()
        if df.empty:
            q = f"select * from {rp_seven_days}.unassigned_totes where  host = '{self.tenant_info['host_name']}'"
            df = pd.DataFrame(self.client.query(q).get_points())
        if not df.empty:
            df = df.sort_values(by=['time'],ascending=[True])
            df = df.groupby('tote_id').last().reset_index()            
            last_cron_ran_at = (pd.to_datetime(df['time'].iloc[-1])-timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
            df2 = pd.merge(df2, df[['tote_id','last_event_time','last_event_name']], on='tote_id', how='left')
            df2['last_event_time'] = df2.apply(lambda x: x['created_at'] if pd.isna(x['last_event_time'])  else x['last_event_time'] , axis = 1)
        else:
            last_cron_ran_at = (datetime.now()-timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
        q = f"select time as tote_events_time , tote_id, event as last_event_name,host,installation_id from tote_events where time>'{last_cron_ran_at}' and host = '{self.tenant_info['host_name']}'"
        tote_events = pd.DataFrame(self.read_client.query(q).get_points())
        if tote_events.empty:
            tote_events = pd.DataFrame(columns=['tote_events_time','tote_id','last_event_name'])
        tote_events = tote_events.groupby('tote_id').last().reset_index()
        final = pd.merge(df2, tote_events, on = ['tote_id'], how = 'left')
        if 'last_event_name_y' in final.columns:
            final.rename(columns = {'last_event_name_y':'last_event_name'}, inplace=True)
            final['last_event_name'] = final.apply(lambda x: x['last_event_name_x'] if pd.isna(x['last_event_name']) else x['last_event_name'], axis = 1)
        if 'last_event_time' not in final.columns:
            final['last_event_time'] = ''
        final['last_event_time'] = final.apply(lambda x: x['tote_events_time'] if not pd.isna(x['tote_events_time']) else x['last_event_time'], axis = 1)
        final['last_event_time'] = final.apply(lambda x: x['created_at'] if pd.isna(x['last_event_time']) else x['last_event_time'], axis = 1)

        self.write_data_to_influx(final)
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'unassigned_totes',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Sku in unassigned totes',
    schedule_interval = '*/15 * * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False,
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['unassigned_totes'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                Operator_working_time_final_task = PythonOperator(
                    task_id='unassigned_totes_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(UnassignedTotes().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Operator_working_time_final_task = PythonOperator(
            task_id='unassigned_totesy_final',
            provide_context=True,
            python_callable=functools.partial(UnassignedTotes().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )