# -----------------------------------------------------------------------------
# Import deps
# -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
import sys
# from GOMail import send_mail
# from GOTimeStamps import get_time_interval
import time
import warnings
import gspread
import re

warnings.filterwarnings("ignore")
from pandasql import sqldf
import numpy as np
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name = os.environ.get('Out_db_name', 'airflow')


#
# dag = DAG(
#     'manual_interventions',
#     default_args = default_args,
#     description = 'calculation of manual_interventions GM-44036',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class manual_interventions:
    def time_diff_rows(self, df):
        for i in df.index:
            if i > 0:
                if (df['butler_id'][i] == df['butler_id'][i - 1]):
                    df['Time_Diff_rows'][i] = self.utilfunction.value_in_sec(df['error_start_time'][i],
                                                                             df['Error_End_Time'][i - 1])
                else:
                    df['Time_Diff_rows'][i] = None
            else:
                pass
        return df

    def data_formatting(self, fieldname, df):
        df[fieldname] = df[fieldname].replace(" ", "\ ")
        df[fieldname] = df[fieldname].replace(".", "\.")
        df[fieldname] = df[fieldname].replace("=", "\=")
        return df

    def manual_interventions(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.utilfunction = CommonFunction()
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("manual_interventions", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        #q = f"select *  from butler_nav_error where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        q = "select  butler_id,direction,value,expected_barcode from butler_nav_error where time>='" + check_start_date + "' and time<'" + check_end_date + "' and error_flag =~ /{8,0,0,0,0,0,0,*/ limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if not df.empty:
            try:
                daterange = self.client.get_datetime_interval3("manual_interventions", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.manual_interventions1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
    def correct_manual_interventions(self, df):
        for index, row in df.iterrows():
            column_value = row['error_flag']
            match = re.match(r'{8,0,0,0,0,0,0,(\d+)}$', str(column_value))

            if match:
                x_value = int(match.group(1))
                df.at[index, 'manual_interventions'] = 1
                if x_value > 0 and (index - x_value) >= 0:
                    extracted_row = df.iloc[index - x_value]
                    df.at[index, 'L1'] = extracted_row['L1']
                    df.at[index, 'L2'] = extracted_row['L2']
        return df

    def manual_interventions1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("manual_interventions", self.tenant_info)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        self.start_date = pd.to_datetime(self.start_date).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.read_influxclient = self.read_client.influx_client
        #qry1 = f"select * from butler_nav_error where time >= '{self.start_date}' and time < '{self.end_date}' order by time desc"
        if 'HNMCANADA' in self.tenant_info["Name"].upper():
            qry1 = f"select * from butler_nav_error where time >= '{self.start_date}' and time < '{self.end_date}'  AND (error_flag !~ /E_SAFE_CONTROL/   and error_flag !~ /E_RANGER_IN_PAUSED/  AND error_flag !~ /E_TIMEOUT/)  and host = '{self.tenant_info['host_name']}' order by time desc"
        else:
            qry1 = f"select * from butler_nav_error where time >= '{self.start_date}' and time < '{self.end_date}' and host = '{self.tenant_info['host_name']}' order by time desc"
        # ---------------------------------------------------------------------------------------------------------------------------
        bna = self.read_client.fetch_data(self.read_influxclient, qry1)
        bna = pd.DataFrame(bna)
        flag = False
        if 'query' in bna.columns:
            if 'No data fetched' in bna['error'][0]:
                flag = True

        if not flag:
            bna['time'] = pd.to_datetime(bna['time'])
            bna['time'] = bna['time'].apply(lambda x: x.to_pydatetime())
            bna['expected_barcode'] = bna['expected_barcode'].astype('string')
            bna['expected_barcode'] = bna.apply(
                lambda x: x['old_barcode'] if pd.isna(x['expected_barcode']) or 'notfound' in x[
                    'expected_barcode'] or 'undefined' in x[
                                                  'expected_barcode'] else x[
                    'expected_barcode'], axis=1)
            bna["direction"] = bna["direction"].astype('string')
            bna['old_barcode'] = bna['old_barcode'].astype('string')
            bna.sort_values(by=['time'], inplace=True)
            # to be used to make join to create carryover file
            # bna = bna.reset_index()
            bna = self.utilfunction.reset_index(bna)
            bna['butler_id'] = bna['butler_id'].astype(int)
            df_sheet = self.utilfunction.get_error_data_flag_sheet()
            df_sheet = df_sheet[(df_sheet['L1'] != '')]
            df_sheet = df_sheet[(pd.isna(df_sheet['L1']) == False)]
            df_sheet["checkflag"] = df_sheet['Is_auto_resolved'].apply(lambda x: 1 if x == "Yes" or x == "*" else 0)
            df_sheet = df_sheet[(df_sheet['checkflag'] == 0)]
            del df_sheet["checkflag"]
            if 'Is_auto_resolved' in bna.columns:
                del bna['Is_auto_resolved']

            if 'L1' in bna.columns:
                del bna['L1']

            if 'L2' in bna.columns:
                del bna['L2']
            bna = pd.merge(bna, df_sheet, left_on=['error_flag'], right_on=['Error Flag'], how='left')
            # ####################################333
            bna['L1'] = bna['L1'].apply(lambda x: "null" if pd.isna(x) or len(x) == 0 else x)
            bna['L2'] = bna['L2'].apply(lambda x: "null" if pd.isna(x) or len(x) == 0 else x)
            bna['Is_auto_resolved'] = bna['Is_auto_resolved'].apply(
                lambda x: "Yes" if pd.isna(x) or '*' in x or x == "" else x)
            # # ############################################333
            # # #fetch data from grid barcode mapping
            bna['manual_interventions'] = 0
            bna = self.correct_manual_interventions(bna)
            bna= bna[(bna['manual_interventions'] == 1)]
            if not bna.empty:
                self.read_client1 = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                               db="Alteryx")
                self.read_influxclient1 = self.read_client1.influx_client
                q = f"select cron_ran_at from rp_seven_days.grid_barcode_mapping where time >now()-30d and host = '{self.tenant_info['host_name']}'  order by time desc limit 1"
                cron_run_at = self.read_client1.fetch_data(self.read_influxclient1, q)
                if 'error' in cron_run_at[0].keys():
                    print("error in query")
                else:
                    cron_run_at_date = cron_run_at[0]["cron_ran_at"]
                    q = f"select barcode, coordinate,zone,category from rp_seven_days.grid_barcode_mapping where time >= '{cron_run_at_date}' and cron_ran_at = '{cron_run_at_date}' and host = '{self.tenant_info['host_name']}'"
                    df_gridbarcode = pd.DataFrame(self.read_client1.query(q).get_points())

                    if df_gridbarcode.empty:
                        q = f"select barcode, coordinate,zone,category from rp_seven_days.grid_barcode_mapping where cron_ran_at = '{cron_run_at_date}' and host = '{self.tenant_info['host_name']}' "
                        df_gridbarcode = pd.DataFrame(self.read_client1.query(q).get_points())

                    bna['expected_barcode'] = bna['expected_barcode'].apply(
                        lambda x: x.replace('{', '[') if not pd.isna(x) else x)
                    bna['expected_barcode'] = bna['expected_barcode'].apply(
                        lambda x: x.replace('}', ']') if not pd.isna(x) else x)
                    if not df_gridbarcode.empty:
                        bna1 = pd.merge(bna, df_gridbarcode, left_on=['expected_barcode'], right_on=['barcode'],
                                        how='inner')
                        if bna1.empty:
                            bna = pd.merge(bna, df_gridbarcode, left_on=['expected_barcode'], right_on=['coordinate'],
                                           how='left')
                        else:
                            bna = pd.merge(bna, df_gridbarcode, left_on=['expected_barcode'], right_on=['barcode'],
                                           how='left')

                        bna['barcode'] = bna['barcode'].apply(lambda x: None if pd.isna(x) else x)
                        bna['barcode_group'] = bna['category'].apply(lambda x: None if pd.isna(x) else x)
                        bna['zone'] = bna['zone'].apply(lambda x: None if pd.isna(x) else x)

                    l = ['index', 'expected_coordinate', 'host', \
                         'old_barcode', 'old_coordinate', 'value', 'barcode_length',
                         'butler_version', 'cstep_coordinate', 'expected_coordinate_attribute',
                         'old_coordinate_attribute', 'flag', \
                          'Error Flag', 'Error Flag Definition', 'Error Categories',
                         'Considered as Stop?', 'Error group', 'Hardware Error', 'category', '', 'coordinate',
                         'error_start_time', 'Error_Start_Time_NS']

                    for col in l:
                        if col in bna.columns:
                            del bna[col]

                    bna.rename(columns={'error': 'error_name', 'time_x': 'time'}, inplace=True)

                    bna.drop(['time_y'], axis=1, inplace=True)
                    ##############################################333
                    bna.time = pd.to_datetime(bna.time)
                    bna['Is_auto_resolved'] = bna['Is_auto_resolved'].astype(str)
                    bna['L1'] = bna['L1'].astype(str)
                    bna['L2'] = bna['L2'].astype(str)
                    bna['barcode'] = bna['barcode'].astype(str)
                    bna['barcode_group'] = bna['barcode_group'].astype(str)
                    bna['butler_id'] = bna['butler_id'].astype(str)
                    bna['error_flag'] = bna['error_flag'].astype(str)
                    bna['error_name'] = bna['error_name'].astype(str)
                    bna['expected_barcode'] = bna['expected_barcode'].astype(str)
                    bna['zone'] = bna['zone'].astype(str)
                    bna["direction"] = bna['direction'].astype(str)
                    bna = bna.set_index('time')
                    self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                         port=self.tenant_info["write_influx_port"])
                    self.write_client.writepoints(bna, "manual_interventions", db_name=self.tenant_info["alteryx_out_db_name"],
                                                  tag_columns=['butler_id', 'error_name'],
                                                  dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        else:
            filter = {"site": self.tenant_info['Name'], "table": "manual_interventions"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)
        return None


# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
        'manual_interventions',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of manual_interventions',
        schedule_interval='52 * * * *',
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
            if tenant['Active'] == "Y" and tenant['manual_interventions'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                manual_interventions_final_task = PythonOperator(
                    task_id='manual_interventions_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(manual_interventions().manual_interventions,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        manual_interventions_final_task = PythonOperator(
            task_id='manual_interventions_final',
            provide_context=True,
            python_callable=functools.partial(manual_interventions().manual_interventions,
                                              tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )
