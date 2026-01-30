## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
import pandas as pd
import joblib
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
from config import (
    CONFFILES_DIR
)
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name =os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class capacity_planning:
    def capacity_planning_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        try:
            daterange = self.client.get_datetime_interval2("capacity_planning_output", '1d',"00:00:00")
            for i in daterange.index:
                    self.start_date = daterange['start_date'][i]
                    self.end_date = daterange['end_date'][i]
                    self.capacity_planning_final1(**kwargs)
        except Exception as e:
            print(f"error:{e}")

    def group_data(self, df):
        data = df.groupby(['working_date', 'working_time', 'bin_tags', 'status'], as_index=False).agg(
            to_be_picked_Item=('to_be_picked_Item', 'last'), new_orders=('new_orders', 'last'))
        data = data.groupby(['working_date', 'working_time'], as_index=False).agg(
            to_be_picked_Item=('to_be_picked_Item', 'sum'), new_orders_Item=('new_orders', 'sum'))
        return data

    def is_feastival_season_day(self, date_month, date_day):
        is_feastival_season = 0
        if (date_month == 11 and date_day > 21) or (date_month == 12):
            is_feastival_season = 1
        if (date_month == 12 and date_day in [22, 23, 24, 25, 26, 27]):
            is_feastival_season = 0
        if (date_month == [4, 10]):
            is_feastival_season = 2
        return is_feastival_season

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df

    def group_data2(self, df):
        data = df.groupby(['working_date', 'bin_tags', 'status'], as_index=False).agg(
            to_be_picked_Item=('to_be_picked_Item', 'last'))
        data = data.groupby(['working_date'], as_index=False).agg(to_be_picked_Item=('to_be_picked_Item', 'sum'))
        return data

    def capacity_planning_final1(self, **kwargs):
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
        print(f"{self.start_date},{self.end_date}")
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q= f"select * from outstanding_orders where time>='{self.start_date}' and time <'{self.end_date}'  order by time desc"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if not df.empty:
            df['working_date'] = df['time'].apply(lambda x: x[0:10])
            df['working_time'] = df['time'].apply(lambda x: x[11:13])
            df['new_orders'] = df.apply(lambda x: x['to_be_picked_Item'] if 'CREATED' in x['status'].upper() else 0,
                                        axis=1)
            df = self.group_data(df)
            df['last_1_day'] = df['working_date'].apply(lambda x: pd.to_datetime(x))
            df['working_date1'] = df['working_date'].apply(lambda x: pd.to_datetime(x) + timedelta(days=1))
            df['dayofweek'] = df['working_date1'].apply(lambda x: x.weekday())
            df['is_weekend'] = df['dayofweek'].apply(lambda x: 1 if x == 6 or x == 5 else 0)
            df['date_day'] = df['working_date1'].apply(lambda x: x.day)
            df['date_month'] = df['working_date1'].apply(lambda x: x.month)
            df['date_year'] = df['working_date1'].apply(lambda x: x.year)
            df['last_monday'] = df.apply(
                lambda x: pd.to_datetime(x['working_date1']) - timedelta(days=x['dayofweek']) if x['dayofweek'] != 0
                else pd.to_datetime(x['working_date1']) - timedelta(days=7), axis=1)
            df.rename(columns={'to_be_picked_Item': 'last_1_day_to_be_picked_Item',
                               'new_orders_Item': 'last_1_day_new_orders_Item'}, inplace=True)
            df['is_feastival_season'] = df.apply(lambda x: self.is_feastival_season_day(x['date_month'], x['date_day']),
                                                 axis=1)
            df['last_1_day_is_feastival_season'] = df['last_1_day'].apply(lambda x: self.is_feastival_season_day(x.month, x.day))
            last_day_open_orders = df.groupby(['last_1_day'], as_index=False).agg(prev_day_open_picked_Item=('last_1_day_to_be_picked_Item', 'last'))
            df = pd.merge(df, last_day_open_orders, on='last_1_day', how='left')
            tempdf = df['last_monday']
            tempdf = tempdf.drop_duplicates(keep='last')
            tempdf = pd.DataFrame(tempdf, columns=['last_monday'])
            tempdf['start_date'] = tempdf['last_monday'].dt.normalize() + pd.Timedelta(hours=0, minutes=0, seconds=0)
            tempdf['end_date'] = tempdf['last_monday'].dt.normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)
            tempdf = tempdf.reset_index()
            last_friday_final_df = pd.DataFrame()
            for i in tempdf.index:
                st = tempdf['start_date'][i]
                et = tempdf['end_date'][i]
                query = f"select * from outstanding_orders where time >='{st}' and time<='{et}' order by time desc"
                print(query)
                last_friday = pd.DataFrame(self.read_client.query(query).get_points())
                last_friday['working_date'] = last_friday['time'].apply(lambda x: x[0:10])
                last_friday = self.group_data2(last_friday)
                if last_friday_final_df.empty:
                    last_friday_final_df = last_friday
                else:
                    last_friday_final_df = pd.concat([last_friday_final_df, last_friday])
            last_friday_final_df.rename(
                columns={'working_date': 'last_monday', 'to_be_picked_Item': 'last_monday_open_picked_Item'},
                inplace=True)
            last_friday_final_df['last_monday'] = last_friday_final_df['last_monday'].apply(lambda x: pd.to_datetime(x))
            final_df = pd.merge(df, last_friday_final_df[['last_monday', 'last_monday_open_picked_Item']],
                                on='last_monday', how='left')
            final_df = final_df.drop(columns=['working_date', 'working_date1', 'last_monday'])
            final_df = final_df.drop(columns=['last_1_day'])
            x_test = final_df[['working_time', 'dayofweek', 'prev_day_open_picked_Item',
                               'last_1_day_to_be_picked_Item', 'last_1_day_new_orders_Item',
                               'last_monday_open_picked_Item', 'date_day', 'date_month', 'date_year',
                               'is_weekend', 'is_feastival_season', 'last_1_day_is_feastival_season']]
            # Load the saved model
            print(x_test.columns)
            path = f'{self.tenant_info["Name"]}_random_forest_model.pkl'
            path = os.path.join(CONFFILES_DIR, path)
            rf_loaded = joblib.load(path)

            # Make predictions
            y_pred = rf_loaded.predict(x_test)
            x_test['expected_to_be_picked_Item'] = y_pred

            x_test['Year'] = x_test['date_year']
            x_test['Month'] = x_test['date_month']
            x_test['Day'] = x_test['date_day']
            x_test['Hour'] = x_test['working_time']
            x_test['time'] = pd.to_datetime(x_test[['Year', 'Month', 'Day', 'Hour']])
            x_test = x_test.drop(columns=['Year', 'Month', 'Day', 'Hour'])
            x_test['working_time'] = x_test['working_time'].astype(int)
            x_test['date_day'] = x_test['date_day'].astype(int)
            x_test['date_month'] = x_test['date_month'].astype(int)
            x_test['date_year'] = x_test['date_year'].astype(int)
            x_test['dayofweek'] = x_test['dayofweek'].astype(int)
            x_test['is_feastival_season'] = x_test['is_feastival_season'].astype(int)
            x_test['is_weekend'] = x_test['is_weekend'].astype(int)
            x_test['last_1_day_is_feastival_season'] = x_test['last_1_day_is_feastival_season'].astype(float)
            x_test['prev_day_open_picked_Item'] = x_test['prev_day_open_picked_Item'].astype(float)
            x_test['last_1_day_to_be_picked_Item'] = x_test['last_1_day_to_be_picked_Item'].astype(float)
            x_test['last_1_day_new_orders_Item'] = x_test['last_1_day_new_orders_Item'].astype(float)
            x_test['last_monday_open_picked_Item'] = x_test['last_monday_open_picked_Item'].astype(float)
            x_test['expected_to_be_picked_Item'] = x_test['expected_to_be_picked_Item'].astype(float)
            x_test = x_test.set_index('time')

            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(x_test, "capacity_planning", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            df2= df[['last_1_day','working_time','last_1_day_to_be_picked_Item']]
            df2['Year'] =  df2['last_1_day'].apply(lambda x: x.year)
            df2['Month'] = df2['last_1_day'].apply(lambda x: x.month)
            df2['Day'] = df2['last_1_day'].apply(lambda x: x.day)
            df2['Hour'] = df2['working_time']
            df2['time'] = pd.to_datetime(df2[['Year', 'Month', 'Day', 'Hour']])
            df2 = df2.drop(columns=['Year', 'Month', 'Day', 'Hour','last_1_day','working_time'])
            df2.rename(columns={'last_1_day_to_be_picked_Item': 'to_be_picked_Item'}, inplace=True)
            df2['to_be_picked_Item'] = df2['to_be_picked_Item'].astype(float)
            df2.time = pd.to_datetime(df2.time)
            df2 = df2.set_index('time')
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(df2, "capacity_planning_output", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=[], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return None


with DAG(
    'capacity_planning',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Capacity planning',
    schedule_interval = '0 7 * * *',
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
            if tenant['Active'] == "Y" and tenant['capacity_planning'] == "Y":
                final_task = PythonOperator(
                    task_id='capacity_planning_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(capacity_planning().capacity_planning_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='capacity_planning_final',
            provide_context=True,
            python_callable=functools.partial(capacity_planning().capacity_planning_final,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

