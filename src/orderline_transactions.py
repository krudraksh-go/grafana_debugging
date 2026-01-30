## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd

from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
from pandasql import sqldf
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')


# dag = DAG(
#     'orderline_transactions',
#     default_args = default_args,
#     description = 'calculation of orderline_transactions GM-44036',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class Orderline_transactions:
    def orderline_transactions(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("orderline_transactions", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>='{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.orderline_transactions1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("orderline_transactions", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.orderline_transactions1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def orderline_transactions1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("orderline_transactions", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        self.CommonFunction = CommonFunction()
        q=f"select order_id,item_id,pps_id, installation_id,host,station_type,storage_type,orderline_ids,num_of_orderlines,rack_id  from item_picked  where  time >= '{self.start_date}' and time < '{self.end_date}' order by time desc"
        df=pd.DataFrame(self.read_client.query(q).get_points())

        if not df.empty:
            df['num_of_orderlines'] = df.apply(lambda x: 1 if pd.isna(x['num_of_orderlines']) else x['num_of_orderlines'], axis=1)
            df['orderline_ids'] = df.apply(lambda x: x['item_id'] if pd.isna(x['orderline_ids']) else x['orderline_ids'], axis=1)
            df=df.sort_values(by=['installation_id', 'pps_id', 'order_id', 'item_id'])
            df = df.reset_index()

            ttp_setup = (self.tenant_info['is_ttp_setup']=='Y')
            df = self.CommonFunction.update_station_type(df,ttp_setup)
            df = self.CommonFunction.update_storage_type(df,ttp_setup)   

            df = sqldf('SELECT *, DENSE_RANK() OVER( ORDER BY installation_id,pps_id,order_id,station_type,storage_type) as ntile from df')

            df_transactions= df.groupby(['host','pps_id','order_id','installation_id','station_type','storage_type','ntile'], as_index=False).agg(Transactions=('item_id', 'size'), start_time=('time', 'min'), end_time=('time', 'max'),rack_id = ('rack_id','first'))
            df = df.sort_values(by=['time'])
            df = df.drop_duplicates(subset=['installation_id', 'order_id', 'item_id','station_type','storage_type'], keep='last')
            df_orderline = sqldf("select max(time) AS time, installation_id,pps_id,order_id,item_id,station_type,storage_type,count(distinct orderline_ids) as orderline_count from df  GROUP BY installation_id,pps_id,order_id,host,ntile,station_type,storage_type")
            df_final=pd.merge(df_orderline,df_transactions,on=['installation_id','pps_id','order_id','station_type','storage_type'],how='inner')
            df_final['trans_per_orderline']=df_final['Transactions']/df_final['orderline_count']
            df_final = df_final.rename(columns={'orderline_count': 'no._of_orderlines'})
            df_final = df_final.rename(columns={'Transactions': 'no._of_transactions'})
            df_final.time = pd.to_datetime(df_final.time)
            df_final = df_final.set_index('time')
            df_final=df_final.drop(columns=['ntile', 'item_id'])
            df_final['start_time'] = pd.to_datetime(df_final['start_time'])
            df_final['end_time'] = pd.to_datetime(df_final['end_time'])
            df_final['start_time'] =df_final['start_time'].apply(lambda x :x.strftime("%Y-%m-%d %H:%M:%S"))
            df_final['end_time'] =df_final['end_time'].apply(lambda x :x.strftime("%Y-%m-%d %H:%M:%S"))
            df_final['no._of_orderlines']=df_final['no._of_orderlines'].astype(float)
            df_final['no._of_transactions'] = df_final['no._of_transactions'].astype(float)
            df_final['trans_per_orderline'] = df_final['trans_per_orderline'].astype(float)
            # try:
            #     if 'PROJECT_ONE' in self.tenant_info["Name"].upper():
            #         df_final['order_id'] = df_final['order_id'].astype(str)
            #     else:
            #         df_final['order_id'] = df_final['order_id'].astype(float)
            # except:
            #     pass
            df_final['order_id'] = df_final['order_id'].astype(str)
            df_final['rack_id'] = df_final['rack_id'].astype(str)
            df_final['pps_id'] = df_final['pps_id'].astype(str)
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            try:
                self.write_client.writepoints(df_final, "orderline_transactions", db_name=self.tenant_info["out_db_name"],
                                              tag_columns=['pps_id','station_type','storage_type'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            except:
                #if wrong order id then 0 else order id
                df_final['order_id'] = df_final['order_id'].apply(lambda x: int(x) if str(x).isdigit() else 0)
                df_final['order_id'] = df_final['order_id'].astype(float)
                self.write_client.writepoints(df_final, "orderline_transactions", db_name=self.tenant_info["out_db_name"],
                                              tag_columns=['pps_id','station_type','storage_type'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        else:
            filter = {"site": self.tenant_info['Name'], "table": "orderline_transactions"} 
            new_last_run = {"last_run": self.end_date}
            self.CommonFunction.update_dag_last_run(filter,new_last_run)  
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'Orderline_transactions',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of Orderline transactions',
    schedule_interval = '35 * * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['Orderline_transactions'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Orderline_transactions_final_task = PythonOperator(
                    task_id='orderline_transactions_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(Orderline_transactions().orderline_transactions,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Orderline_transactions_final_task = PythonOperator(
            task_id='orderline_transactions_final',
            provide_context=True,
            #python_callable=ButlerUptime().butler_uptime_final,
            python_callable=functools.partial(Orderline_transactions().orderline_transactions,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

