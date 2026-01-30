## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import pytz
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
from r2r_waittime_calculation import R2rWaittimeCalculation
from operator_working_time_summary import operator_working_time_summary
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

class pick_per_rack_face:
    def divide_slotref(self, df):
        df['rack'] = df['slotref'].str.split('.', expand=True)[0]
        df['face'] = df['slotref'].str.split('.', expand=True)[1]
        return df

    def get_sku_count(self, df):
        df_sku = df.groupby(['installation_id', 'pps_id', 'rack', 'face']).item_id.nunique()
        df_sku = df_sku.reset_index()
        return df_sku

    def get_line_count(self, df):
        df['key'] = 1
        df_line_count = df.groupby(['installation_id', 'pps_id', 'rack', 'face']).key.sum()
        df_line_count = df_line_count.reset_index()
        return df_line_count

    def get_order_count(self, df):
        df_order_count = df.groupby(['installation_id', 'pps_id', 'rack', 'face']).order_id.nunique()
        df_order_count = df_order_count.reset_index()
        return df_order_count

    def get_total_picks_value(self, df):
        df2 = df.groupby(['installation_id', 'pps_id', 'rack', 'face']).value.sum()
        df2 = df2.reset_index()
        return df2

    def get_total_picks(self, df):
        df_total_picks = df.groupby(['installation_id', 'pps_id', 'rack', 'face']).uom_quantity_int.sum()
        df_total_picks = df_total_picks.reset_index()
        return df_total_picks

    def apply_ntile(self, df):
        Group = 1
        for x in df.index:
            if x > 0:
                if df['installation_id'][x] == df['installation_id'][x - 1] and df['pps_id'][x] == df['pps_id'][
                    x - 1] and df['rack'][x] == df['rack'][x - 1] and df['pps_point'][x] == df['pps_point'][x - 1]:
                    df["ntile"][x] = df["ntile"][x - 1]
                else:
                    Group = Group + 1
                    df["ntile"][x] = Group
            else:
                df["ntile"][x] = Group
        return df

    def picks_per_rack_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["out_db_name"])
        #        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("picks_per_rack_face", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>'{check_start_date}' and time<='{check_end_date}' and value > 0 limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.picks_per_rack_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("picks_per_rack_face", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("picks_per_rack_face", '5min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.picks_per_rack_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def date_compare(self, x):
        diff = (x['start'].tz_localize(tz=pytz.UTC) - x['time'].tz_convert(tz=pytz.UTC)).total_seconds()
        # diff = (x['start'].tz_localize(tz=pytz.UTC)-x['interval_start'].tz_localize(tz=pytz.UTC)).total_seconds()
        if diff <= 0:
            return True
        return False

    def picks_per_rack_final1(self, end_date, **kwargs):
        self.utilfunction = CommonFunction()
        self.start_date = self.client.get_start_date("picks_per_rack_face", self.tenant_info)
        temp_data = pd.to_datetime(self.start_date) - timedelta(minutes=10)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.CommonFunction = CommonFunction()

        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select installation_id,pps_id,uom_quantity_int,slotref,value,item_id,order_id,uom_quantity,station_type,storage_type,num_of_orderlines,pps_point,user_id from item_picked where time>'{temp_data}' and time <= '{self.end_date}' and value > 0 order by time desc"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            filter = {"site": self.tenant_info['Name'], "table": "picks_per_rack_face"}
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter, new_last_run)
        if not df.empty:
            df = self.divide_slotref(df)
            df['user_id'] = df['user_id'].apply(lambda x: 'default' if (pd.isna(x) or x=='') else x)
            df.time = pd.to_datetime(df.time)
            df['rack_id'] = df.apply(lambda x: str(x['rack']), axis=1)
            df['rack'] = df.apply(lambda x: str(x['rack']) + '.' + str(x['face']), axis=1)
            if 'pps_point' not in df.columns:
                df['pps_point'] = ''
            else:
                df['pps_point'] = df['pps_point'].apply(lambda x: '' if pd.isna(x) else x)
            df = df.sort_values(by=['installation_id', 'pps_id', 'pps_point', 'time'], \
                                ascending=[True, True, True, True])

            df = df.reset_index()

            df['num_of_orderlines'] = df.apply(
                lambda x: 1 if pd.isna(x['num_of_orderlines']) else x['num_of_orderlines'], axis=1)
            ttp_setup = (self.tenant_info['is_ttp_setup'] == 'Y')
            df = self.CommonFunction.update_station_type(df, ttp_setup)
            df = self.CommonFunction.update_storage_type(df, ttp_setup)

            df["ntile"] = 0
            df = self.apply_ntile(df)
            df['uom_quantity_int'] = df.apply(
                lambda x: int(x['uom_quantity']) if pd.isna(x['uom_quantity_int']) and not pd.isna(
                    x['uom_quantity']) else x['uom_quantity_int'], axis=1)
            df['uom_quantity_int'] = df.apply(
                lambda x: int(x['value']) if pd.isna(x['uom_quantity_int']) else x['uom_quantity_int'], axis=1)
            # df["ntile"] =df[["installation_id","pps_id","rack"]].apply(tuple,axis=1)\
            #      .rank(method='dense',ascending=True).astype(int)
            df = df.groupby(['installation_id', 'pps_id', 'rack', 'ntile', 'station_type', 'storage_type', 'pps_point'],
                            as_index=False).agg(time=('time', 'min'), line_count=('num_of_orderlines', 'sum'),
                                                sku_count=('item_id', 'nunique'),
                                                order_id_count=('order_id', 'nunique'),
                                                total_picks=('uom_quantity_int', 'sum'),
                                                total_picks_value=('value', 'sum'), slot_count=('slotref', 'nunique'),
                                                transaction_count=('installation_id', 'count'),
                                                rack_id = ('rack_id','first'),
                                                operator_id = ('user_id','max')
                                                )
            del df['ntile']
            df['line_count'] = df['line_count'].astype(float)
            df['order_id_count'] = df['order_id_count'].astype(float)
            df['sku_count'] = df['sku_count'].astype(float)
            df['total_picks'] = df['total_picks'].astype(float)
            df['total_picks_value'] = df['total_picks_value'].astype(float)
            df['transaction_count'] = df['transaction_count'].astype(float)
            df['slot_count'] = df['slot_count'].astype(float)
            df['rack'] = df['rack'].astype('str')
            df['rack_id'] = df['rack_id'].astype('str')
            df['pps_id'] = df['pps_id'].astype('str')
            df['installation_id'] = df['installation_id'].astype('str')
            df['pps_point'] = df['pps_point'].astype('str')
            df['operator_id'] = df['operator_id'].astype('str')
            df["start"] = pd.to_datetime(self.start_date) - timedelta(minutes=5)
            df["flag2"] = df.apply(self.date_compare, axis=1)
            df = df[(df["flag2"])]
            if not df.empty:
                df = df.drop(['flag2', 'start'], axis=1)
            df.time = pd.to_datetime(df.time)
            df = df.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df, "picks_per_rack_face", db_name=self.tenant_info["out_db_name"],
                                          tag_columns=['pps_id', 'station_type', 'storage_type'],
                                          dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None


with DAG(
        'Picks_per_rack_face',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of picks per rack face GM-44025',
        schedule_interval='*/5 * * * *',
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
            if tenant['Active'] == "Y" and tenant['Picks_per_rack_face'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                try:
                    final_task = PythonOperator(
                        task_id='picks_per_rack_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(pick_per_rack_face().picks_per_rack_final,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=600),
                    )
                    Client_setting_final_task = PythonOperator(
                        task_id='R2r_Waittime_Calculation_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(R2rWaittimeCalculation().final_call,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=600), )

                    Client_setting_final_task2 = PythonOperator(
                        task_id='operator_working_time_summary_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(operator_working_time_summary().operator_working_time_summary,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=600), )
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='picks_per_rack_final',
            provide_context=True,
            python_callable=functools.partial(pick_per_rack_face().picks_per_rack_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

