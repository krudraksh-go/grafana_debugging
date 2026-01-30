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
import os
import math

from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
from pandasql import sqldf

# ---------------------------------------------------------------------------

class replen:

    def convertToDate(self,time):
        timestamp = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ")
        new_timestamp = timestamp.replace(second=0, microsecond=0)
        return new_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
    def get_product_key(self,df,site_name=''):
        site_name = site_name.upper()
        if 'PROJECT' in site_name:
            df['channel'] = df['product_key'].apply(lambda x: 'retail' if 'W141' in x or 'W146' in x  else 'online')
        elif 'HNMCANADA' in site_name:
            df['channel'] = df['product_key'].apply(lambda x: 'HM retail' if 'W387' in x else ('HM online' if 'W388' in x else 'COS Retail'))
        else:
            df['channel'] = ''

    def product_tag_key_in_standardForm(self,product_tag_key):
        string = "\""
        flag=False
        count=0
        for elements in product_tag_key:
            if elements=="'":
                if count==0 or count==2:
                    flag=True
                elif count==1:
                    flag=False
                    string=string+"\":\""
                else:
                    flag=False
                count=count+1
            else:
                if flag:
                    string = string+elements
        string = string+"\""
        return string

    def contains(self, product_tags, product_tag_key):
        product_tag_key = self.product_tag_key_in_standardForm(product_tag_key)
        if product_tag_key in product_tags:
            return 1
        else:
            return 0

    def check_list1(self, list1,value):
        for index in range(len(list1)-1):
                upper_range = list1[index]
                lower_range = list1[index+1]
                if value<=upper_range and value>lower_range:
                    return str(upper_range)+'_to_'+str(lower_range), chr(9-index+65)  ## chr(9-index+65+alphabet) convert to alphabet

    def check_list2(self,list1, value):
        for index in range(len(list1)-1):
                lower_range = list1[index]
                upper_range = list1[index+1]
                if value>=lower_range and value<upper_range:
                    return str(lower_range)+'_to_'+str(upper_range), chr(13+index+65)  ## chr(9-index+65+alphabet) convert to alphabet

    def categorise_on_value(self,value):
        if value<0 and value>-10:
            return '-1_to_-10', 'k'
        elif value<=-10 and value>-100:
            list1 = [-10,-20,-30,-40,-50,-60,-70,-80,-90,-100]
            return self.check_list1(list1,value)
        elif value<=-100:
            return 'below_-100', 'A'
        elif value==0:
            return "0","L"
        elif value>0 and value<10:
            return '1_to_10','M'
        elif value>=10 and value<100:
            list1 = [10,20,30,40,50,60,70,80,90,100]
            return self.check_list2(list1,value)
        elif value>=100:
            return 'above_100','W'
        else:
            return 'NA','AA'
    
    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db=self.tenant_info["out_db_name"])
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        try:
            daterange = self.client.get_datetime_interval2("replenishment_analysis", '1d',self.tenant_info['StartTime'])
            for i in daterange.index:
                self.start_date = daterange['start_date'][i]
                self.end_date = daterange['end_date'][i]
                self.final_call1(self.start_date, self.end_date, **kwargs)
        except AirflowTaskTimeout as timeout_exception:
            raise timeout_exception                 
        except Exception as e:
            print(f"error:{e}")
            raise e

    def final_call1(self, start_date, end_date, **kwargs):
        self.start_date = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"start_date: {self.start_date}, end_date: {self.end_date}")
        q = f"select installation_id,item_id,product_tags,value as sum_value from item_picked where time >= '{self.start_date}' and time <= '{self.end_date}' and value>0 order by time desc"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        q = f"select first(predicted_quantity) as first, item_id,product_tag_key from proactive_cron_prediction where time >= '{self.start_date}' and time <= '{self.end_date}' group by product_tag_key, item_id"
        df_prediction = pd.DataFrame(self.read_client.query(q).get_points())

        if not df.empty and not df_prediction.empty:

            df_item_picked = df.groupby(['installation_id','item_id','product_tags']).agg({'sum_value':'sum'}).reset_index()

            # picked but not predicted only left data calculated
            df_picked_but_not_predicted = pd.merge(df_item_picked,df_prediction,on=['item_id'],how='left')
            
            df_prediction_and_picked = pd.merge(df_item_picked,df_prediction,on=['item_id'],how='inner')

            df_picked_but_not_predicted = df_picked_but_not_predicted.loc[(pd.isna(df_picked_but_not_predicted['product_tag_key'])==False)]

            df_picked_but_not_predicted.drop(['installation_id','product_tags','first','product_tag_key'],inplace=True,axis=1)
            df_picked_but_not_predicted.rename({'sum_value':'picked'},axis=1,inplace=True)

            df_picked_but_not_predicted['date'] = df_picked_but_not_predicted['time'].apply(lambda x: self.convertToDate(x))

            df_prediction_and_picked['flag'] = df_prediction_and_picked.apply(lambda x: self.contains(x['product_tags'],x['product_tag_key']),axis=1)
            df_key_in_tag = df_prediction_and_picked[df_prediction_and_picked['flag']==1]
            df_key_not_in_tag = df_prediction_and_picked[df_prediction_and_picked['flag']==0]

            # creating only right data i.e rightjoin-innerjoin
            df_right_data = pd.merge(df_key_in_tag,df_key_not_in_tag,on=['product_tags','item_id'],how='right')
            df_right_data = df_right_data.loc[(pd.isna(df_right_data['time_x'])==True)]
            df_right_data = df_right_data.reset_index()

            df_right_data.drop(['installation_id_x','sum_value_x','time_x','first_x','product_tag_key_x','flag_x'],inplace=True,axis=1)
            df_right_data.rename({'installation_id_y':'installation_id','sum_value_y':'sum_value','time_y':'time','first_y':'first','product_tag_key_y':'product_tag_key','flag_y':'flag'},axis=1,inplace=True)
            df_key_not_in_data = df_right_data[(pd.isna(df_right_data['product_tag_key']) == False)]
            final_set = pd.concat([df_key_in_tag, df_key_not_in_data])
            final_set_temp = final_set.groupby(['product_tag_key','item_id']).agg({'sum_value':'sum','first':'first'}).reset_index()
            final_set_temp.rename({'sum_value':'actual_picked','first':'predicted_picked'},axis=1,inplace=True)
            final_set = pd.merge(final_set,final_set_temp,on=['product_tag_key','item_id'],how='inner')

            final_set['deviation'] = final_set['predicted_picked'] - final_set['actual_picked']
            final_set['product_tag_key'].fillna('NO_PK', inplace=True)
            final_set['item_PK'] = final_set['item_id']+'-'+final_set['product_tag_key']
            final_set['deviation_per'] = round((final_set['deviation']/final_set['predicted_picked'])*100, 1)
            final_set['deviation_bin_5']='NA'
            final_set['group']='AA'
            for index in final_set.index:
                final_set['deviation_bin_5'][index], final_set['group'][index] = self.categorise_on_value(final_set['deviation_per'][index])
                final_set['deviation_bin_5'][index] = "("+final_set['group'][index]+")"+final_set['deviation_bin_5'][index]
            final_set['RMSE'] = (final_set['deviation']*final_set['deviation'])

            sum_RMSE = final_set['RMSE'].sum()
            Count = final_set['deviation'].count()
            rmse = round(math.sqrt(sum_RMSE/Count),2)
            final_set['RMSE']=rmse 

            final_set.drop(['installation_id','product_tags','sum_value','first','flag','index'],inplace=True,axis=1)
            final_set.rename({'group':'bucketno','deviation_bin_5':'deviation_bucket','product_tag_key':'product_key'},inplace=True,axis=1)
            final_set['date'] = final_set['time'].apply(lambda x: self.convertToDate(x))
            self.get_product_key(final_set,self.tenant_info['Name'])

            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(final_set, "replenishment_analysis", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            self.write_client.writepoints(df_picked_but_not_predicted, "picked_but_not_predicted", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            print("data inserted")
        
        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'Replen',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Replen',
    schedule_interval = '2 4 * * *',
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
            if tenant['Active'] == "Y" and tenant['replen'] == "Y":
                replen_final_task = PythonOperator(
                    task_id='replen_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(replen().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        replen_final_task = PythonOperator(
            task_id='replen_final',
            provide_context=True,
            python_callable=functools.partial(replen().final_call,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )