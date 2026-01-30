import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import  CommonFunction, Butler_api, Write_InfluxData
import os
import numpy as np
class RackLocationPopularity:
    
    def create_result_df(self, rack_popularity, location_mapping, location_popularity):
        location_popularity['category'] = location_popularity['category'].apply(lambda x: x.upper())
        location_popularity = location_popularity.rename(columns = {'category':'popularity'})
        result = rack_popularity.groupby(['entity_popularity'], as_index=False).agg(
                    entity_count = ('entity_id','count')).reset_index(drop=True)
        result = result.rename(columns = {'entity_popularity':'popularity'})
        location_df = location_popularity.groupby(['popularity'], as_index=False).agg(
                    location_count = ('location','count')).reset_index(drop=True)
        result = pd.merge(result,location_df, on ='popularity', how = 'outer')
        tote_location_popularity = pd.merge(location_mapping,location_popularity, on = 'location', how = 'inner')
        df = pd.merge(tote_location_popularity,rack_popularity, on = 'entity_id', how = 'inner')
        df = df.groupby(['entity_popularity','popularity'], as_index=False).agg(
                entity_count = ('entity_id','count')).reset_index()
        df = df.pivot_table(index='entity_popularity',columns='popularity',values='entity_count',fill_value=0).reset_index()
        df.columns.name = None
        df = df.rename(columns = {'entity_popularity':'popularity'})
        cols = ['A', 'B', 'C', 'D']
        for col in cols:
            if col not in df.columns:
                df[col] = 0
        result = pd.merge(result, df, on = 'popularity',how = 'outer')
        result = result.fillna(0)
        total_entities = result['entity_count'].sum()
        total_locations = result['location_count'].sum()
        result['entity_percentage'] = result['entity_count'].apply(lambda x : round(100* x/total_entities,2))
        result['location_percentage'] = result['location_count'].apply(lambda x : round(100*x /total_locations,2))
        for col in cols:
            result[col] = result[col].apply(lambda x : round(100*x/total_entities,10))
        for col in cols:
            result['variance_'+col] = abs(result['location_percentage'] - result[col])
        return result
    def fillnullvalues(self, df):
        for col in df.columns:
            if 'variance' in col:
                df[col] = df[col].fillna(100)
            else:
                df[col] = df[col].fillna(0)
        df = df.fillna(0)
        return df
    def set_cron_ran_time(self, df):
        df['cron_ran_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        return df
    def set_time_index(self, df):
        df['time'] = datetime.now(timezone.utc)
        curr_date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        df['date'] = curr_date
        df = df.set_index(pd.to_datetime(np.arange(len(df)), unit='s', origin=df['date'][0]))
        cols_to_delete = ['date', 'time']
        for col in cols_to_delete:
            if col in df.columns:
                del df[col]
        return df
    def final_call(self, tenant_info):
        self.tenant_info = tenant_info['tenant_info']
        self.butler = Butler_api(host=self.tenant_info['Butler_ip'],port=self.tenant_info['Butler_port'])
        rack_score = self.butler.fetch_rack_popularity_data()
        tote_location_popularity = self.butler.fetch_tote_location_popularity()[['location','category']]
        rack_location_popularity = self.butler.fetch_rack_location_popularity()

        if not rack_location_popularity.empty:
            rack_location_popularity['category'] = rack_location_popularity['score'].apply(lambda x : 'A' if x<0.3 else( 'B' if x<0.7 else 'C'))
            rack_location_popularity = rack_location_popularity[['location','category']]
        
        rack_score = rack_score.rename(columns={'rack_id': 'entity_id', 'popularity_category':'entity_popularity'})
        rack_score['entity_type'] = rack_score['entity_id'].apply(lambda x:'tote' if x[0].isalpha() else 'rack')
        rack_popularity = rack_score[rack_score['entity_type']=='rack'][['entity_id','entity_popularity']]
        tote_popularity = rack_score[rack_score['entity_type']=='tote'][['entity_id','entity_popularity']]
        location_mapping = self.butler.fetch_location_mapping_data()  
        tote_location_mapping = location_mapping[location_mapping['entity_type']=='tote'].reset_index(drop=True)
        rack_location_mapping = location_mapping[location_mapping['entity_type']=='rack'].reset_index(drop=True)
        res_cols = ['popularity', 'entity_count', 'location_count', 'A', 'B', 'C', 'D','entity_percentage', 'location_percentage', 'variance_A', 'variance_B','variance_C', 'variance_D', 'entity_type']

        if not tote_location_popularity.empty:
            totes_result = self.create_result_df(tote_popularity,tote_location_mapping,tote_location_popularity)
            totes_result['entity_type'] = 'tote'
        else:
            totes_result = pd.DataFrame(columns=res_cols)
        
        if not rack_location_popularity.empty:
            rack_result = self.create_result_df(rack_popularity,rack_location_mapping,rack_location_popularity)
            rack_result['entity_type'] = 'rack'
        else:
            rack_result = pd.DataFrame(columns=res_cols)

        result = pd.concat([totes_result,rack_result]).reset_index(drop=True)

        result['host'] = self.tenant_info['host_name']
        result['installation_id'] = self.tenant_info['installation_id']
        result['host'] = result['host'].astype(str)
        result['installation_id'] = result['installation_id'].astype(str)
        result['location_count'] = result['location_count'].astype(float)

        result = self.fillnullvalues(result)
        result = self.set_cron_ran_time(result)
        result = self.set_time_index(result)

        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(result, "rack_location_popularity", db_name=self.tenant_info["alteryx_out_db_name"],
                                        tag_columns=['popularity','entity_type'],
                                        dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        print("inserted")
with DAG(
        'Rack_location_popularity',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of popularity of rack and location',
        schedule_interval='36 * * * *',
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
            if tenant['Active'] == "Y" and tenant['Rack_location_popularity'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='rack_location_popularity_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(RackLocationPopularity().final_call,
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
            task_id='rack_location_popularity_final',
            provide_context=True,
            python_callable=functools.partial(RackLocationPopularity().final_call,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )