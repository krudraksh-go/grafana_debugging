## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

from doctest import DocFileSuite
import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import pytz
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, postgres_connection
from config import (
    rp_seven_days
)
import numpy as np
import os
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class tote_history:

    def typecast_cols(self, df):
        cols = ['quantity','tote_id','last_transaction_time','last_transaction_type','last_clear_time','last_audit_time','cron_ran_at','last_pick_time','last_put_time']
        for col in cols:
            if col not in df.columns:
                df[col] = np.nan
            df[col] = df[col].fillna('-').astype(str,errors='ignore')
        df = df[cols]
        return df        
    
    def fetch_past_data(self):
        cron_ran_at = self.fetch_last_cron_ran_at(f"{rp_seven_days}.tote_history", self.client)
        q = f"select * from {rp_seven_days}.tote_history where time>='{cron_ran_at}' and host ='{self.tenant_info['host_name']}'"
        print(q)
        df = pd.DataFrame(self.client.query(q).get_points())
        if df.empty:
            return pd.DataFrame(columns=['tote_id', 'quantity', 'last_transaction_time', 'last_transaction_type', 'last_clear_time', 'last_audit_time', 'cron_ran_at','last_pick_time','last_put_time'])
        return df    

    def fetch_data_tote(self):
        self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                sslrootcert=self.tenant_info['sslrootcert'],
                                                sslcert=self.tenant_info['sslcert'], \
                                                sslkey=self.tenant_info['sslkey'],
                                                host=self.tenant_info['Postgres_tower'], \
                                                port=self.tenant_info['Postgres_tower_port'],
                                                password=self.tenant_info['Tower_password'],
                                                dag_name=os.path.basename(__file__),
                                                site_name=self.tenant_info['Name'])
        qry = f"select tote_id from data_tote  "
        print(qry)
        data = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
        self.postgres_conn.close()
        df = pd.DataFrame(data, columns=['tote_id'])
        return df
    
    def is_tote_history_table_exists(self):
        q = f"show measurements"
        df = pd.DataFrame(self.client.query(q).get_points())
        if 'tote_history' in df['name'].values:
            return True
        return False        

    def fetch_last_cron_ran_at(self, table_name, client):
        old_date = (pd.to_datetime(self.end_date) - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        q = f"select cron_ran_at from {table_name} where time>'{old_date}' order by time desc limit 1"
        df = pd.DataFrame(client.query(q).get_points())
        if df.empty:
            return ''
        cron_ran_at = df['cron_ran_at'][0]
        return cron_ran_at

    def fetch_slots_ageing_elk(self):
        cron_ran_at = self.fetch_last_cron_ran_at(f"{rp_seven_days}.slots_ageing_elk", self.read_client)
        q = f"select remaining_uom as quantity, last_transaction_time, last_sku_transaction_event as last_transaction_type, slot_id as tote_id from {rp_seven_days}.slots_ageing_elk where time>='{cron_ran_at}' and host='{self.tenant_info['host_name']}' order by time desc"
        print(q)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            return pd.DataFrame(columns=['quantity', 'last_transaction_time', 'last_transaction_type', 'tote_id'])
        df['tote_id'] = df['tote_id'].apply(lambda x : x.split('.')[0])
        df = df.groupby('tote_id').agg({'quantity': 'last', 'last_transaction_time': 'last', 'last_transaction_type': 'last'}).reset_index()
        df['quantity'] = df['quantity'].apply(lambda x : x.split(':')[1].replace('}', '').replace(' ','') if isinstance(x, str) and len(x.split(':')) > 1 else np.nan)
        if 'time' in df.columns:
            del df['time']
        return df
    
    def fetch_inventory_transaction_archives(self):
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
        q = f""" SELECT slot as tote_id,
                    transaction_type as last_transaction_type,
                    new_uom::text AS quantity,
                    time AS last_transaction_time
                FROM (
                    SELECT slot,
                        transaction_type,
                        new_uom,
                        time,
                        ROW_NUMBER() OVER (PARTITION BY slot, transaction_type ORDER BY time DESC) AS rn
                    FROM inventory_transaction_archives
                    WHERE time >= '{self.start_date}'
                    AND time <= '{self.end_date}'
                ) t
                WHERE rn = 1 order by time; """ ;
        print(q)
        df = pd.DataFrame(self.postgres_conn.fetch_postgres_data_in_chunk(q), columns=['tote_id', 'last_transaction_type', 'quantity', 'last_transaction_time'])
        if df.empty:
            return pd.DataFrame(columns=['tote_id', 'last_transaction_type', 'quantity', 'last_transaction_time','last_pick_time','last_put_time'])
        
        cols = ['pick','put']
        for col in cols:
            df[col] = ''
            df[col] = df.apply(lambda x : x['last_transaction_time'] if x['last_transaction_type']==col else x[col], axis = 1)
            df[col] = df[col].astype(str)
        df = df.fillna('')
        df = df.groupby('tote_id').agg({'quantity': 'last', 'last_transaction_time': 'last', 'last_transaction_type': 'last','pick':'max','put':'max'}).reset_index()
        df = df.rename(columns = {'pick':'last_pick_time', 'put':'last_put_time'})
        df['tote_id'] = df['tote_id'].apply(lambda x : x.split('.')[0])
        df = df[['tote_id','last_pick_time','last_put_time','last_transaction_type','last_transaction_time','quantity']]
        df['quantity'] = df.quantity.astype(str).str.extract('(\d+)}$', expand=False)
        df['quantity'] =df['quantity'].apply(lambda x:0 if pd.isna(x) else x)
        return df

    def fetch_tote_empty_data(self):
        q = f"select tote_id, time as last_clear_time from tote_events where time>='{self.start_date}' and time <= '{self.end_date}' and  destination_type = 'conveyor_blackhole' and host='{self.tenant_info['host_name']}' "
        print(q)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            return pd.DataFrame(columns=['tote_id', 'last_clear_time'])
        df = df.groupby('tote_id').agg({'last_clear_time': 'last'}).reset_index()
        return df

    def fetch_audit_events(self):
        q = f"select rack_id as tote_id, time as last_audit_time from auditline_events where time>='{self.start_date}' and time <= '{self.end_date}'  and (event_name = 'audit_pending_approval' or event_name = 'audit_completed')  and host='{self.tenant_info['host_name']}'"
        print(q)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            return pd.DataFrame(columns=['tote_id', 'last_audit_time'])
        df = df.groupby('tote_id').agg({'last_audit_time': 'last'}).reset_index()
        return df
    
    def write_to_influx(self, df):
        df['cron_ran_at'] = self.end_date
        df = self.typecast_cols(df)
        df = self.CommonFunction.get_epoch_time(df, date1=self.end_date)
        df = df.set_index('time_ns')
        if 'index' in df.columns:
            del df['index']
        df['host'] = self.tenant_info['host_name']
        df['installation_id'] = self.tenant_info['installation_id']
        df['host'] = df['host'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "tote_history", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=[],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy = rp_seven_days)   
        print("inserted")     

    def tote_history_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"]) 
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db=self.tenant_info["out_db_name"])
        self.CommonFunction = CommonFunction()

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')


        check_start_date = self.client.get_start_date(f"{rp_seven_days}.tote_history", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f""" select * from inventory_transaction_archives where time>'{check_start_date}' and time<='{check_end_date}' limit 1 """ 
        print(q)
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
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        df = pd.DataFrame(data, columns=[col[0] for col in self.postgres_conn.pg_curr.description])
        self.postgres_conn.close()

        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.tote_history_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3(f"{rp_seven_days}.tote_history", '4h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3(f"{rp_seven_days}.tote_history", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.tote_history_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def tote_history_final1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date(f"{rp_seven_days}.tote_history", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date:{self.end_date}")

        data_tote = self.fetch_data_tote()
        if not self.is_tote_history_table_exists():
            slots_ageing_elk = self.fetch_slots_ageing_elk()
            df = pd.merge(data_tote, slots_ageing_elk, on='tote_id', how='left')      
            self.write_to_influx(df)
            return None

        inventory_transaction_archives = self.fetch_inventory_transaction_archives()
        df = pd.merge(data_tote, inventory_transaction_archives, on='tote_id', how='left')

        
        if df.empty:
            return None
        
        tote_empty_data = self.fetch_tote_empty_data()
        df = pd.merge(df, tote_empty_data, on='tote_id', how='left')

        audit_events = self.fetch_audit_events()
        df = pd.merge(df, audit_events, on='tote_id', how='left')

        df['last_transaction_time'] = pd.to_datetime(df['last_transaction_time'])
        df['last_audit_time'] = pd.to_datetime(df['last_audit_time'])
        df['update_audit_time'] = df.apply(lambda x : 1 if ( (~pd.isna(x['last_audit_time'])) & ((pd.isna(x['last_transaction_time'])) | (x['last_transaction_time']<x['last_audit_time'])) ) else 0, axis = 1)
        df['last_transaction_time'] = df.apply(lambda x : x['last_audit_time'] if x['update_audit_time']==1 else x['last_transaction_time'], axis = 1)
        df['last_transaction_type'] = df.apply(lambda x : 'audit' if x['update_audit_time']==1 else x['last_transaction_type'], axis = 1)

        past_data = self.fetch_past_data()
        df = pd.merge(df, past_data, on='tote_id', how='left', suffixes=("", "_df2"))
        for col in ["quantity", "last_transaction_time", "last_transaction_type", "last_clear_time", "last_audit_time","last_pick_time","last_put_time"]:
            df[col] = df.apply(lambda x : x[f"{col}_df2"] if (pd.isna(x[col]) or x[col] in ['-','']) else x[col], axis = 1)
            df.drop(columns=[f"{col}_df2"], inplace=True)

        self.write_to_influx(df)
        return None
    


with DAG(
        'tote_history',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='tote_history dag created',
        schedule_interval='15 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(seconds=3600),
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['tote_history'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='tote_history_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(tote_history().tote_history_final,
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
            task_id='tote_history_final',
            provide_context=True,
            python_callable=functools.partial(tote_history().tote_history_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

