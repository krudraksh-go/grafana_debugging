## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

from calendar import c
import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import pytz
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, Butler_api, GCS, MongoDBManager
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
from config import (
    rp_one_year,
    MongoDbServer,
)

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class sku_velocity:                                             
    
    def fetch_wms_data(self):
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        collection_name = self.tenant_info['Name'] + "_sku_details"
        cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
        data = cls_mdb.get_data()
        cls_mdb.close_connection()
        df = pd.DataFrame(data)
        cols = ['item_id', 'sku_id', 'max_quantity', 'min_quantity', 'max_DOH', 'min_DOH']
        df['item_id'] = df['item_id'].astype(str)
        if df is None or df.empty:
            df =  pd.DataFrame(columns=cols)
        return df[cols]
    
    def fetch_left_inventory_per_item(self):
        q = f"select installation_id,item_id, remaining_item_int from rp_seven_days.slots_ageing_elk where time>'{self.start_date}' and time <= '{self.end_date}' and host = '{self.tenant_info['host_name']}' order by time desc"
        print(q)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        cols = ['installation_id','item_id','remaining_item_int']
        if df.empty:
            return pd.DataFrame(columns=cols)
        df = df[cols]            
        df = df.groupby(['installation_id','item_id'], as_index=False).agg({'remaining_item_int': 'last'}).reset_index()
        df['item_id'] = df['item_id'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)
        return df.reset_index(drop=True)

    def log_consolidated_item_picked(self):
        q = f"select value, item_id,installation_id from item_picked where time>'{self.start_date}' and time <= '{self.end_date}' and host = '{self.tenant_info['host_name']}' "
        print(q)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            df = pd.DataFrame(columns=['installation_id','item_id', 'today_picked'])
        df = df.groupby(['installation_id','item_id'], as_index=False).agg({'value': 'sum'}).reset_index()
        q = f"select * from proactive_cron_prediction_v2 where time>'{self.start_date}' and time <= '{self.end_date}'"
        df_prediction = pd.DataFrame(self.read_client.query(q).get_points())
        if df_prediction.empty:
            df_prediction = pd.DataFrame(columns=['installation_id','item_id', 'predicted_quantity'])
        df_prediction = df_prediction.groupby(['installation_id','item_id'], as_index=False).agg({'predicted_quantity': 'sum'}).reset_index()
        df = pd.merge(df, df_prediction, on=['installation_id','item_id'], how='outer')
        if df.empty:
            return pd.DataFrame()
        if 'predicted_quantity' not in df.columns:
            df['predicted_quantity'] = 0
        df = df.fillna(0)
        df = self.CommonFunction.get_epoch_time(df, self.end_date)
        df = df.set_index('time_ns')
        if 'index' in df.columns:
            del df['index']
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "consolidated_item_picked", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=[],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy=rp_one_year)
        df = df[df['value'] > 0].reset_index(drop=True)
        df['item_id'] = df['item_id'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)
        return df[['installation_id', 'item_id', 'today_picked']]
    
    def fetch_velocity(self):
        old_date = (pd.to_datetime(self.end_date) - timedelta(days=13)).strftime('%Y-%m-%d')
        q = f"select * from {rp_one_year}.consolidated_item_picked where time>='{old_date}' and time < '{self.end_date}' and host = '{self.tenant_info['host_name']}' "
        print(q)
        df = pd.DataFrame(self.client.query(q).get_points())
        if df.empty:
            return pd.DataFrame(columns=['installation_id','item_id', 'velocity'])
        df = df.groupby(['installation_id','item_id'], as_index=False).agg({'value': 'sum'}).reset_index()
        df = df.rename(columns={'value': 'velocity'})
        df['velocity'] = df['velocity'].apply(lambda x: round(x/14, 2))
        df['item_id'] = df['item_id'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)
        return df
    
    def fetch_last_day_picked(self):
        yesterday = (pd.to_datetime(self.end_date) - timedelta(days=1)).strftime('%Y-%m-%d')
        q = f"select item_id,installation_id from {rp_one_year}.consolidated_item_picked where time>='{yesterday}' and time < '{self.end_date}' and host = '{self.tenant_info['host_name']}'"
        print(q)
        df = pd.DataFrame(self.client.query(q).get_points())
        if df.empty:
            return pd.DataFrame(columns=['installation_id','item_id', 'last_day_picked'])
        df['last_day_picked'] = 1
        df['item_id'] = df['item_id'].astype(str)
        return df
    
    def convert_dtype(self, df):
        float_cols = ['doh', 'velocity', 'remaining_item_int','max_DOH','min_DOH','min_quantity','max_quantity']
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
                df[col] = df[col].astype(float)
        str_cols = ['sku_id', 'item_id','popularity_bucket']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna('')
                df[col] = df[col].astype(str)
        int_cols = ['last_day_picked', 'today_picked']
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].astype(int)        
        return df

    def sku_velocity_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"]) 
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db=self.tenant_info["out_db_name"])
        self.butler = Butler_api(host=self.tenant_info['Butler_ip'], port=self.tenant_info['Butler_port'])                                      

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("sku_velocity", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.sku_velocity_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("sku_velocity", '1d', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.sku_velocity_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e        
    

    def sku_velocity_final1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("sku_velocity", self.tenant_info)
        self.start_date = pd.to_datetime(self.start_date).strftime('%Y-%m-%d')
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%d')

        print(f"start_date: {self.start_date}, end_date:{self.end_date}")

        self.CommonFunction = CommonFunction()
        today_picked = self.log_consolidated_item_picked()

        if today_picked.empty:
            filter = {"site": self.tenant_info['Name'], "table": "sku_velocity"}
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter, new_last_run)
            return None

        item_popularity = self.butler.fetch_item_popularity_data()
        wms_data = self.fetch_wms_data()
        inventory_left = self.fetch_left_inventory_per_item()
        velocity = self.fetch_velocity()
        last_day_picked = self.fetch_last_day_picked()
        
        df = pd.merge(item_popularity, wms_data, on='item_id', how='left')
        df = pd.merge(df, inventory_left, on='item_id', how='left')
        df = pd.merge(df, velocity, on=['item_id','installation_id'], how='left')
        df = pd.merge(df, last_day_picked, on=['item_id','installation_id'], how='left')
        df = pd.merge(df, today_picked, on=['item_id','installation_id'], how='left')

        df['velocity'] = df['velocity'].fillna(0).astype(int)
        df['last_day_picked'] = df['last_day_picked'].fillna(0).astype(int)
        df['remaining_item_int'] = df['remaining_item_int'].fillna(0).astype(int)
        df['doh'] = df.apply(lambda x : round(x['remaining_item_int']/x['velocity'], 2) if x['velocity'] >0 and x['remaining_item_int']>0 else 0, axis = 1)
        df['doh'] = df['doh'].apply(lambda x: round(x, 2))
        df['today_picked'] = df['today_picked'].fillna(0).astype(int)

        final_df = df.groupby(['installation_id','popularity_bucket'], as_index=False).agg({'doh': 'mean', 'velocity': 'mean','remaining_item_int': 'sum'}).reset_index()
        df = self.convert_dtype(df)
        final_df = self.convert_dtype(final_df)

        df = self.CommonFunction.get_epoch_time(df, self.end_date)
        final_df = self.CommonFunction.get_epoch_time(final_df, self.end_date)

        df = df.set_index('time_ns')
        final_df = final_df.set_index('time_ns')

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "sku_velocity", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['popularity_bucket'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        self.write_client.writepoints(final_df, "category_velocity", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['popularity_bucket'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])                                      
        return None


with DAG(
        'sku_velocity',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='sku_velocity dag created',
        schedule_interval='0 2 * * *',
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
            if tenant['Active'] == "Y" and tenant['sku_velocity'] == "Y"   and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='sku_velocity_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(sku_velocity().sku_velocity_final,
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
            task_id='sku_velocity_final',
            provide_context=True,
            python_callable=functools.partial(sku_velocity().sku_velocity_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

