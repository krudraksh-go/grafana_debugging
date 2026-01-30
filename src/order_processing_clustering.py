## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, MongoDBManager
import json
from config import (
    MongoDbServer,rp_seven_days
    )
import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class OrderProcessingClustering:

    def get_prev_data(self, collection_name, connection_string=MongoDbServer, database_name="GreyOrange"):
        cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
        prev_data = cls_mdb.get_data()
        cls_mdb.close_connection()
        prev_data = pd.DataFrame(prev_data)
        if prev_data is None or prev_data.empty:
            prev_data =  pd.DataFrame(columns=['installation_id', 'pps_id', 'aisle_id', 'total_cluster_per_pps_per_aisle', 'total_cluster_per_pps'])
        return prev_data
    
    def save_to_mongodb(self, df, collection_name, connection_string=MongoDbServer, database_name="GreyOrange"):
        cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
        if not df.empty:
            json_list = df.to_json(orient="records")
            json_list = json.loads(json_list)
            cls_mdb.delete_data({},multiple=True)  # delete all previous data
            inserted_ids = cls_mdb.insert_data(json_list)
        cls_mdb.close_connection()

    def delete_unnecessary_columns(self, df, cols):
        for col in cols:
            if col in df.columns:
                del df[col]     

    def create_consolidate_df(self, df, prev_df):
        consolidate_df_per_pps_per_aisle = df.groupby(['pps_id','aisle_id','installation_id'], as_index=False).agg(
                            new_cluster_per_pps_per_aisle = ('total_cluster_per_pps_aisle','last')
                            )
        consolidate_df_per_pps = df.groupby(['pps_id','installation_id'], as_index=False).agg(
                            new_cluster_per_pps = ('total_cluster_per_pps','last'),
                            )    
        # consolidate_df = pd.merge(consolidate_df_per_pps_per_aisle, consolidate_df_per_pps, on=['pps_id', 'installation_id'], how='left')   
        consolidate_df = pd.merge(prev_df,consolidate_df_per_pps_per_aisle,on = ['pps_id','aisle_id','installation_id'], how = 'outer')
        consolidate_df2 = pd.merge(consolidate_df, consolidate_df_per_pps, on=['pps_id', 'installation_id'], how='left')   
        consolidate_df2['cluster_per_pps_per_aisle'] = consolidate_df2.apply(lambda x: x['new_cluster_per_pps_per_aisle'] if not pd.isna(x['new_cluster_per_pps_per_aisle']) else x['cluster_per_pps_per_aisle'], axis = 1)
        consolidate_df2['cluster_per_pps'] = consolidate_df2.apply(lambda x: x['new_cluster_per_pps'] if not pd.isna(x['new_cluster_per_pps']) else x['cluster_per_pps'], axis=1)
        cols = ['installation_id','pps_id','aisle_id','cluster_per_pps_per_aisle','cluster_per_pps']
        consolidate_df2 = consolidate_df2[cols]
        return consolidate_df2        


    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        #        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date(f"{rp_seven_days}.order_processing_clustering", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from order_processing_clustering_result where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3(f"{rp_seven_days}.order_processing_clustering", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3(f"{rp_seven_days}.order_processing_clustering", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        
        self.start_date = self.client.get_start_date(f"{rp_seven_days}.order_processing_clustering", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date: {self.end_date}")
        
        # mongodb connection
        connection_string = MongoDbServer
        database_name = "GreyOrange"
        collection_name = self.tenant_info['Name'] + "_order_processing_clustering"

        q = f"select * from order_processing_clustering_result where time>'{self.start_date}' and time <= '{self.end_date}' order by time"
        df = pd.DataFrame(self.read_client.query(q).get_points())

        # if empty dataframe, update last run and return
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": f"{rp_seven_days}.order_processing_clustering"}
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter, new_last_run)
            return

        df['sign'] = -1
        df['sign'] = df['event_type'].apply(lambda x: 1 if x=='created' else -1)
        prev_df = self.get_prev_data(collection_name, connection_string, database_name)

        per_pps_per_aisle_map  = {}
        per_pps_map = {}
        for index, row in prev_df.iterrows():
            per_pps_per_aisle_index = row['installation_id']+'_'+row['pps_id']+'_'+row['aisle_id']
            per_pps_index = row['installation_id']+'_'+row['pps_id']            
            per_pps_per_aisle_map[per_pps_per_aisle_index] = row['cluster_per_pps_per_aisle']
            per_pps_map[per_pps_index] = row['cluster_per_pps']
        df = df.sort_values(by=['installation_id','pps_id','time',], ascending=[True, True, True]).reset_index(drop=True)
        df['total_cluster_per_pps'] = 0
        df['total_cluster_per_pps_aisle'] = 0      
        for index, row in df.iterrows():
            per_pps_index = row['installation_id']+'_'+row['pps_id']
            per_pps_per_aisle_index = per_pps_index+'_'+row['aisle_id']
            sign = row['sign']
            if per_pps_per_aisle_index not in per_pps_per_aisle_map:
                per_pps_per_aisle_map[per_pps_per_aisle_index] = 0
            if per_pps_index not in per_pps_map:
                per_pps_map[per_pps_index] = 0
            per_pps_per_aisle_map[per_pps_per_aisle_index] = max(per_pps_per_aisle_map[per_pps_per_aisle_index]+sign,0)
            per_pps_map[per_pps_index] = max(per_pps_map[per_pps_index]+sign,0)
            
            df.at[index,'total_cluster_per_pps'] = per_pps_map[per_pps_index]
            df.at[index,'total_cluster_per_pps_aisle'] = per_pps_per_aisle_map[per_pps_per_aisle_index]    

        consolidate_df = self.create_consolidate_df(df, prev_df)
        
        # write to consolidate_df mongodb
        self.save_to_mongodb(consolidate_df, collection_name, connection_string, database_name)
        
        df.time = pd.to_datetime(df.time)
        df = df.set_index('time')

        cols_to_del = ['sign']
        self.delete_unnecessary_columns(df, cols_to_del)

        int_cols = ['total_cluster_per_pps', 'total_cluster_per_pps_aisle']
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].astype(int)

        # write to influxdb
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "order_processing_clustering", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['aisle_id', 'pps_id'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], retention_policy=rp_seven_days)
        print("inserted")
        return None


with DAG(
        'Order_processing_clustering',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='order_processing_clustering',
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
            if tenant['Active'] == "Y" and tenant['order_processing_clustering'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                try:
                    final_task = PythonOperator(
                        task_id='Order_processing_clustering_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(OrderProcessingClustering().final_call,
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
            task_id='Order_processing_clustering_final',
            provide_context=True,
            python_callable=functools.partial(OrderProcessingClustering().final_call,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

