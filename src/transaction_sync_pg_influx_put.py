## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import os
import psycopg2
import psycopg2.extras as extras
import numpy as np
import json
from airflow.exceptions import AirflowTaskTimeout
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, postgres_connection


# ---------------------------------------------------------------------------------------------

class TransactionSyncPGInfluxPUT():     

    def convert_to_ns(self, df,col):
        df[col] = df[col].apply(lambda x: x.timestamp())
        df[col] = df[col]* 1e9
        return df

    def del_columns(self, df, col):
        if col in df.columns:
            del df[col]
        return df

    def convert_to_dtype(self, df , cols, dtype, fill_value):
        df[cols] = df[cols].fillna(value = fill_value)
        df[cols] = df[cols].replace('None', fill_value)
        df[cols] = df[cols].replace('', fill_value)
        df[cols] = df[cols].replace('-', fill_value)
        
        for col in cols:
            df[col] = df[col].astype(dtype, errors='ignore')
        return df

    def create_columns(self, df, columns):
        for col in columns:
            if col not in df.columns:
                if col in ['attr_installation_id','attr_host']:
                    df[col] = "butler_demo"     
                elif col=='attr_fulfilment_area':
                    df[col] = "gtp"             
                else:
                    df[col] = np.nan
        return df    


    def expand_json_column(self, raw_df, col="attributes", drop=True, prefix="attr"):
        df = raw_df.copy(deep=True)
        parsed = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        expanded = pd.json_normalize(parsed, sep="_")
        expanded = expanded.add_prefix(f"{prefix}_")
        df = pd.concat([df.reset_index(drop=True), expanded.reset_index(drop=True)], axis=1)
        if drop:
            df = df.drop(columns=[col])
        return df            


    def process_put_data(self, df):
        rename_map = {
            'attr_pps_bin_id':'bin_id',
            'attr_pps_id':'pps_id',
            'id':'put_id',
            'attr_put_type' : 'put_type',
            'quantity':'quantity_int',
            'attr_user_name':'user_id',
            'updated_on':'time',
            'external_service_request_id':'external_sr_id',
            "attr_fulfilment_area":'fulfilment_area',
            "attr_station_type":'station_type',
            "attr_storagetype":'storagetype',
            "attr_installation_id":'installation_id',
            "attr_host":'host'
        }   
        df = df.rename(columns = rename_map)

        df['value'] = df['quantity_int']
        columns = ['time','bin_id','external_sr_id','item_id','pps_id','put_id','put_type','quantity_int','uom','uom_quantity_int','user_id','value','fulfilment_area','station_type','storagetype','installation_id','host']

        df = df[columns]   

        df['time'] = pd.to_datetime(df['time'])  + pd.to_timedelta(df['put_id'].astype(int), unit='ns')
        df = df.set_index('time')

        int_col_type_cast = ['value','uom_quantity_int','quantity_int']
        df = self.convert_to_dtype(df, int_col_type_cast, int, 0)

        int_col_str_type_cast = ['bin_id','item_id','pps_id','put_id']
        df = self.convert_to_dtype(df, int_col_str_type_cast, int, 0)
        df = self.convert_to_dtype(df, int_col_str_type_cast, str, '')

        str_columns = ['external_sr_id','uom','user_id','fulfilment_area','station_type','storagetype','installation_id','host']
        df = self.convert_to_dtype(df, str_columns, str, '-')
        df['host'] = self.tenant_info['host_name']
        df['installation_id'] = self.tenant_info['installation_id']
        df['host'] = df['host'].astype(str)
        df['installation_id'] = df['installation_id'].astype(str)
        
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "item_put", db_name=self.tenant_info["out_db_name"],tag_columns=['pps_id','station_type','storagetype','installation_id','user_id','fulfilment_area','host'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None


    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.CommonFunction = CommonFunction()

        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("item_put", self.tenant_info, where_clause_list=[" installation_id = 'butler_demo' "])
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f""" select * from service_request where updated_on>'{check_start_date}' and updated_on<='{check_end_date}' and status in('PROCESSED') and  "type" = 'PUT' limit 1  """
        print(q)
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        df = pd.DataFrame(data, columns=[col[0] for col in self.postgres_conn.pg_curr.description])
        self.postgres_conn.close()

        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("item_put", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("item_put", self.tenant_info, where_clause_list=[" installation_id = 'butler_demo' "])
        self.end_date = end_date.replace(second=0)
        print(f"start_date: {self.start_date}, end_date: {self.end_date}")

        self.postgres_conn = postgres_connection(database='platform_srms',
                                                 user=self.tenant_info['Postgres_pf_user'], \
                                                 sslrootcert=self.tenant_info['sslrootcert'],
                                                 sslcert=self.tenant_info['sslcert'], \
                                                 sslkey=self.tenant_info['sslkey'],
                                                 host=self.tenant_info['Postgres_pf'], \
                                                 port=self.tenant_info['Postgres_pf_port'],
                                                 password=self.tenant_info['Postgres_pf_password'], \
                                                 dag_name=os.path.basename(__file__), \
                                                 site_name=self.tenant_info['Name'])


        q = f""" select sr.*, ((sr.attributes::json->>'order_options')::json->>'bintags')::json->>1 as bin_tags, \
                su.quantity AS quantity, su.possibleuids::json -> 1 -> 0 ->> 'product_uid' AS item_id, \
                su.attributes::json->>'package_name' as uom_picked, su.attributes::json->>'package_count' as uom_quantity_int\
                FROM service_request sr \
                LEFT JOIN service_request_expectations sre \
                    ON sr.id = sre.service_request_id \
                LEFT JOIN container_stock_units csu \
                    ON sre.expectations_id = csu.container_id \
                LEFT JOIN stock_unit su \
                    ON csu.stockunits_id = su.id \
                WHERE sr.updated_on>'{self.start_date}' and sr.updated_on<'{self.end_date}' AND status in('PROCESSED') and "type" = 'PUT'  """

        print(q)
        
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        df = pd.DataFrame(data, columns=[col[0] for col in self.postgres_conn.pg_curr.description])
        self.postgres_conn.close()
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "item_put"}
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter, new_last_run)
            return None
        df = self.expand_json_column(df, col="attributes", drop=True, prefix="attr")

        necessary_columns = ['put_type','uom','attr_slotref','attr_installation_id','attr_station_type','attr_storagetype','attr_fulfilment_area','attr_host']
        df = self.create_columns(df, necessary_columns)

        print("put data started")
        self.process_put_data(df)
        print("put data completed")
