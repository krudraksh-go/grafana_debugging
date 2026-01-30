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

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, postgres_connection, \
    tableau_postgres_setup

from config import (
    rp_one_year
)


# ---------------------------------------------------------------------------------------------

class sla_debugging():

    def convert_to_ns(self, df,col):
        df[col] = df[col].apply(lambda x: x.timestamp())
        df[col] = df[col]* 1e9
        return df

    def del_columns(self, df, col):
        if col in df.columns:
            del df[col]
        return df

    def dateconversion(self,input_string):
        if pd.isna(input_string):
            return input_string
        if not isinstance(input_string, str):
            input_string = str(input_string)
        data = input_string.strip("[]").split(", ")
        try:
            result = data[-1]
            if len(data)>1:
                result = float(result)
                result = datetime.utcfromtimestamp(result)
            else:
                result = pd.to_datetime(result, utc=True)
            print(input_string,result)
            return result.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as ex:
            print(f"unable to convert {input_string} to y-m-d h-m-s format error : {ex}")

    def expand_json_column(self, raw_df, col="attributes", drop=True, prefix="attr"):
        df = raw_df.copy(deep=True)
        parsed = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        expanded = pd.json_normalize(parsed, sep="_")
        expanded = expanded.add_prefix(f"{prefix}_")
        df = pd.concat([df.reset_index(drop=True), expanded.reset_index(drop=True)], axis=1)
        if drop:
            df = df.drop(columns=[col])
        return df            
        
    def write_data_to_influx(self, df):
        df['CT'] = df['CT'].apply(self.dateconversion)
        df['pbt2'] = df['PBT'].apply(lambda x:pd.to_datetime(x).strftime('%Y-%m-%d') if (not pd.isna(x)) else x)
        df['PBT'] = df['PBT'].apply(self.dateconversion)
        df['created_on'] = df['created_on'].apply(self.dateconversion)
        df['updated_on'] = df['updated_on'].apply(self.dateconversion)
        df['time'] = ''
        df['time'] = df.apply(lambda x: x['created_on'] if x['status']=='CREATED' else x['updated_on'], axis = 1)
        df['time'] = pd.to_datetime(df['time'],utc=True)
        df = df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                        port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "order_pbt", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['pbt2'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

    def update_value(self, df):
        site = self.tenant_info['Name']
        sql = """
            update sla_debugging u
            set
                updated_on = t.updated_on,
                PBT = t.PBT,
                CT = t.CT, 
                AT = t.AT,
                order_type=t.order_type,
                pps_id=t.pps_id,
                pps_bin_id=t.pps_bin_id,
                bin_tags=t.bin_tags
            from (values %s) as t(site, created_on, external_service_request_id, id, updated_on, PBT, CT, AT, order_type, pps_id, pps_bin_id, bin_tags)
            where u.id = t.id and u.site =t.site;
        """

        insert_sql = """
            insert into sla_debugging(site, created_on, external_service_request_id, id, updated_on, PBT, CT, AT, order_type, pps_id, pps_bin_id, bin_tags)
            select t.site, t.created_on, t.external_service_request_id, t.id, t.updated_on, t.PBT, t.CT, t.AT, t.order_type, t.pps_id, t.pps_bin_id, t.bin_tags
            from (values %s) as t(site, created_on, external_service_request_id, id, updated_on, PBT, CT, AT, order_type, pps_id, pps_bin_id, bin_tags)
            left join sla_debugging as u
            on u.id = t.id and u.site =t.site where u.id is null;
        """
        # insert_sql = """
        #     insert into sla_debugging(site, created_on, external_service_request_id, id, updated_on, PBT, CT, AT, order_type, pps_id, pps_bin_id, bin_tags)
        #     select t.site, t.created_on, t.external_service_request_id, t.id, t.updated_on, t.PBT, t.CT, t.AT, t.order_type, t.pps_id, t.pps_bin_id, t.bin_tags
        #     from (values %s) as t(site, created_on, external_service_request_id, id, updated_on, PBT, CT, AT, order_type, pps_id, pps_bin_id, bin_tags)
        #     ON CONFLICT (id,site) DO NOTHING;
        # """

        rows_to_update = df[
            ['site', 'created_on', 'external_service_request_id', 'id', 'updated_on', 'PBT', 'CT', 'AT', 'order_type',
             'pps_id', 'pps_bin_id', 'bin_tags']].values.tolist()
        conn = self.tableau_func.connection_to_consolidatedb()
        cursor = conn.cursor()  # Assuming you already got the connection object
        extras.execute_values(cursor, insert_sql, rows_to_update)
        conn.commit()
        extras.execute_values(cursor, sql, rows_to_update)
        conn.commit()
        return 1

    def delete_data_old_data(self):
        # SQL query to execute
        conn = self.tableau_func.connection_to_consolidatedb()
        day_14_old_date = datetime.now(timezone.utc) - timedelta(days=14)
        day_14_old_date = day_14_old_date.strftime("%Y-%m-%d")
        site = self.tenant_info['Name']
        query = f"DELETE FROM sla_debugging WHERE updated_on<='{day_14_old_date}' and site = '{site}';"
        print(query)
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("the dataframe is delete data")
        cursor.close()

    def process_pick_data(self, raw_df):
        df = raw_df.copy(deep=True)
        df = df[df['type'].isin(['PICK','PICK_LINE'])].reset_index(drop=True)
        if df.empty:
            print("No Pick Data")
            return None
        rename_map = {
            'attr_pps_id':'pps_id',
            'attr_user_name' : 'operator',
            'attr_pps_bin_id':'bin_id',
            'attr_order_type':'order_type',
            'attr_pick_before_time':'pick_before_time',
            'attr_allocation_time':'allocation_time',
            'attr_completion_time':'completion_time',
            'attr_is_short_pick':'is_short_pick',
            'attr_released_time':'released_time',
            'attr_rollcage_id' : 'rollcage_id',
            'attr_packing_box_id':'packing_box_id',
            'attr_store_number' : 'store_number',
            'attr_order_date':'order_date',            
        }
        df = df.rename(columns = rename_map)

        df['order_id'] = df.apply(lambda x : x['attr_sr_parent'] if x['attr_has_parent'] == True else x['id'], axis = 1)
        df['orderline_id'] = df.apply(lambda x : x['id'] if x['type'] == 'PICK_LINE' else '', axis = 1)
        df['item_id'] = df['item_id'].fillna(0).astype(int).astype(str)
        df['sku'] = df['sku'].apply(lambda x : x.split('=')[1].strip("'") if isinstance(x, str) else '-') 


        columns = ['pps_id','order_id','orderline_id','operator','quantity',
                    'item_id','bin_id','order_type','bin_tags','created_on','updated_on',
                    'pick_before_time','allocation_time','completion_time',
                    'state','is_short_pick','released_time',
                    'type','external_service_request_id','rollcage_id',
                    'packing_box_id','store_number','sku' ,'order_date','id']

        df = df[columns]
        epoch_time_fields = ['allocation_time','completion_time','pick_before_time','released_time','created_on','updated_on']
        zero_datetime = pd.to_datetime(datetime.utcfromtimestamp(0).strftime('%Y-%m-%d %H:%M:%S'))
        for col in epoch_time_fields:
            df[col] = df[col].apply(lambda x: pd.to_datetime(x) if (not pd.isna(x) and x!= '-') else zero_datetime)
            df = self.convert_to_ns(df,col)
        
        df['time'] = pd.to_datetime(df['created_on']) + pd.to_timedelta(df['id'].astype(int), unit='ns')
        df = df.set_index('time')

        int_cols = ['allocation_time', 'completion_time', 'created_on', 'released_time', 'updated_on', 'pick_before_time', 'quantity','order_date']
        df = self.convert_to_dtype(df, int_cols, int, 0)
        
        str_columns = ['bin_id','bin_tags','external_service_request_id','is_short_pick','operator','pps_id','orderline_id','order_id','item_id','order_type','packing_box_id','sku','state','store_number','type','rollcage_id']
        df = self.convert_to_dtype(df, str_columns, str, '-')
        df['bin_tags'] = df['bin_tags'].apply(lambda x: x.replace('"', ''))
        
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "service_request_pick", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy=rp_one_year)
        return None

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
                df[col] = np.nan
        return df

    def process_put_data(self, raw_df):
        df = raw_df.copy(deep=True)
        df = df[df['type'].isin(['PUT'])].reset_index(drop=True)
        if df.empty:
            print("No Put Data")
            return None
        rename_map = {
            'id':'put_id',
            'attr_pps_id':'pps_id',
            'attr_toteId':'tote_id',
            'attr_flow_name':'flow_name',
            'attr_user_name':'operator',
            'attr_order_type':'order_type',
            'attr_pps_bin_id':'bin_id',
            'attr_allocation_time':'allocation_time',
            'attr_completion_time':'completion_time',
            'attr_put_type' : 'put_type',
            'attr_put_method' : 'put_method',
        }
        df = df.rename(columns = rename_map)

        columns = ['put_id','created_on','external_service_request_id',
                    'received_on','state','status','type',
                    'updated_on','quantity', 'item_id',
                    'pps_id','tote_id','flow_name','operator',
                    'order_type','bin_id',
                    'allocation_time','completion_time',
                    'put_type','put_method']
        df = df[columns]
        zero_datetime = pd.to_datetime(datetime.utcfromtimestamp(0).strftime('%Y-%m-%d %H:%M:%S'))
        epoch_time_fields = ['allocation_time','completion_time','received_on','created_on','updated_on']
        for col in epoch_time_fields:
            df[col] = df[col].apply(lambda x: pd.to_datetime(x) if (not pd.isna(x) and x!= '-') else zero_datetime)
            df = self.convert_to_ns(df,col)      

        df['time'] = pd.to_datetime(df['created_on']) + pd.to_timedelta(df['put_id'].astype(int), unit='ns')
        df = df.set_index('time')
 
        int_cols = ['allocation_time', 'completion_time', 'created_on', 'quantity', 'received_on', 'updated_on']
        df = self.convert_to_dtype(df, int_cols, int, 0)
        str_columns = ['bin_id','external_service_request_id','flow_name','operator','put_method','put_type','state','status','tote_id','type','order_type','item_id', 'pps_id', 'put_id',]
        df = self.convert_to_dtype(df, str_columns, str, '-')
        
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "service_request_put", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy=rp_one_year)
        return None


    def final_call(self, tenant_info, **kwargs):

        self.tenant_info = tenant_info['tenant_info']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        self.tableau_func = tableau_postgres_setup()
        consolidate_db = self.tableau_func.connection_to_consolidatedb()

        q = f"select * from sla_debugging where site = '{self.tenant_info['Name']}' order by updated_on desc limit 1"
        cursor = consolidate_db.cursor()
        cursor.execute(q)
        last_entry = cursor.fetchall()
        cursor.close()

        last_entry = pd.DataFrame(last_entry,
                                  columns=["site", "created_on", "external_service_request_id", "id", "updated_on",
                                           "PBT", "CT", "AT", "order_type", "pps_id", "pps_bin_id", "bin_tags",
                                           "timestamp"])
        if last_entry.empty:
            start = datetime.now(timezone.utc) - timedelta(days=10)
            # start = start.strftime("%Y-%m-%d %H:%M:%S")
        else:
            start = last_entry['updated_on'][0]
            start = pd.to_datetime(start, utc=True)
        end = datetime.now(timezone.utc)
        daterange = CommonFunction().create_time_series_data2(start, end, '1h')
        for i in daterange.index:
            end_date = daterange['interval_end'][i]
            self.final_call1(end_date)

        # delete data before 14 days
        self.delete_data_old_data()

    def process_sla_debugging(self, raw_df):
        df = raw_df.copy(deep=True)       
        df = df[df['type'] == 'PICK']
        if df.empty:
            print("No Pick Data for SLA Debugging")
            return None
        df = df.rename(columns = { 'attr_order_type':'order_type', 'attr_pps_id':'pps_id', 'attr_pps_bin_id':'pps_bin_id'})

        columns=["created_on", "status", "external_service_request_id", "id", "updated_on", "PBT", "CT", "AT",
                                   "order_type", "pps_id", "pps_bin_id", "bin_tags"]        
        df = df[columns]
        df['site'] = self.tenant_info['Name']
        df['external_service_request_id'] = df['external_service_request_id'].str.slice(0, 100)
        df['order_type'] = df['order_type'].fillna('').astype(str, errors='ignore')
        df['pps_id'] = df['pps_id'].replace('', np.nan)
        df['pps_bin_id'] = df['pps_bin_id'].replace('', np.nan)

        df2 = df.copy()
        df2 = df2[df2['status']!='PROCESSING'].reset_index(drop=True)
        df2['bin_tags'] = df2['bin_tags'].apply(lambda x: x[:100] if isinstance(x, str) else x)
        if 'status' in df2.columns:
            del df2['status']
        if not df.empty:
            self.update_value(df2)
            # self.write_data_to_influx(df)
        return None

    def final_call1(self, end, **kwargs):
        q = f"select updated_on from sla_debugging where site = '{self.tenant_info['Name']}' order by updated_on desc limit 1"
        consolidate_db = self.tableau_func.connection_to_consolidatedb()
        cursor = consolidate_db.cursor()
        cursor.execute(q)
        last_entry = cursor.fetchall()
        cursor.close()

        last_entry = pd.DataFrame(last_entry,columns=["updated_on"])
        if last_entry.empty:
            start = datetime.now(timezone.utc) - timedelta(days=10)
            start = start.strftime("%Y-%m-%d %H:%M:%S")
        else:
            start = last_entry['updated_on'][0]
        end = end.strftime("%Y-%m-%d %H:%M:%S")
        print(f"start: {start}, end: {end}")

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
                sr.attributes::json->>'pick_before_time' as "PBT",sr.attributes::json->>'completion_time' as "CT", sr.attributes::json->>'allocation_time' as "AT", \
                su.quantity AS quantity, su.possibleuids::json -> 1 -> 0 ->> 'product_uid' AS item_id, \
                su.attributes::json -> 'filter_parameters' -> 1 -> 0 as sku \
                FROM service_request sr \
                LEFT JOIN service_request_expectations sre \
                    ON sr.id = sre.service_request_id \
                LEFT JOIN container_stock_units csu \
                    ON sre.expectations_id = csu.container_id \
                LEFT JOIN stock_unit su \
                    ON csu.stockunits_id = su.id \
                WHERE sr.updated_on>'{start}' and sr.updated_on<'{end}' AND status in('PROCESSED','PROCESSING','CREATED')  """

        print(q)
        
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        raw_df = pd.DataFrame(data, columns=[col[0] for col in self.postgres_conn.pg_curr.description])
        self.postgres_conn.close()
        if raw_df.empty:
            return None
        raw_df = self.expand_json_column(raw_df, col="attributes", drop=True, prefix="attr")

        necessary_columns = ['attr_order_date','attr_order_type','attr_allocation_time','attr_completion_time',
        'attr_released_time','attr_toteId','attr_put_type','attr_put_method','attr_pps_id', 'attr_pps_bin_id','attr_user_name','attr_sr_parent',
        'attr_is_short_pick', 'attr_rollcage_id', 'attr_packing_box_id', 'attr_store_number']

        raw_df = self.create_columns(raw_df, necessary_columns)

        print("sla debugging started")
        self.process_sla_debugging(raw_df)
        print("sla debugging completed")
        # print("pick data started")
        # self.process_pick_data(raw_df)
        # print("pick data completed")
        # print("put data started")
        # self.process_put_data(raw_df)
        # print("put data completed")
        # print("all data processed")



# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
        'SLA_debugging',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='sla-debugging',
        schedule_interval='43 * * * *',
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
            if tenant['Active'] == "Y" and tenant['sla_debugging'] == "Y" and tenant['is_production'] == "Y":
                sla_debugging_task = PythonOperator(
                    task_id='sla_debugging_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(sla_debugging().final_call, tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        sla_debugging_final_task = PythonOperator(
            task_id='sla_debugging_final',
            provide_context=True,
            python_callable=functools.partial(sla_debugging().final_call, tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )