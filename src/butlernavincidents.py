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
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, GCS
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

import os, json
from utils.CommonFunction import MongoDBManager
from config import (
    GOOGLE_AUTH_JSON,
    CONFFILES_DIR,
    GOOGLE_BUCKET_NAME,
    MongoDbServer,
    rp_seven_days
)

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name = os.environ.get('Out_db_name', 'airflow')


#
# dag = DAG(
#     'butler_nav_incidents',
#     default_args = default_args,
#     description = 'calculation of butler_nav_incidents GM-44036',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class butler_nav_incidents:
    def put_flag(self, df):
        for i in df.index:
            if i >= 0 and i != df.index[-1]:
                if ((df['butler_id'][i] == df['butler_id'][i + 1]) and (
                        (df['error_flag'][i] == '{1,0,0,0,0,0,0,0}') and (
                        df['error_flag'][i + 1] == '{1,0,0,0,0,0,0,0}'))):
                    df['flag'][i] = 1
                else:
                    df['flag'][i] = 0
            else:
                df['flag'][i] = 0

        return df

    def alter_flag(self, df):
        for i in df.index:
            if i >= 0 and i != df.index[-1]:
                if ((df['error_flag'][i] == '{1,0,0,0,0,0,0,0}') and (
                        df['butler_id'][i] == df['butler_id'][i + 1]) and (
                        (df['error_flag'][i + 1] == '{0,0,0,0,0,0,8,1}') or (
                        df['error_flag'][i + 1] == '{0,0,0,0,0,0,8,2}') or (
                                df['error_flag'][i + 1] == '{0,0,0,0,0,0,4,1}'))):
                    df['error_flag'][i] = df['error_flag'][i + 1]
                    df['error'][i] = df['error'][i + 1]
                    df['direction'][i] = df['direction'][i + 1]
                    df['expected_barcode'][i] = df['expected_barcode'][i + 1]
                else:
                    pass
            else:
                pass
        return df

    def copy_data(self, df):
        for i in df.index:
            if i >= 0 and i != df.index[-1]:
                if (df['alart_flag'][i] == 1):
                    df['expected_barcode'][i] = df['expected_barcode'][i + 1]
                    df['error_flag'][i] = df['error_flag'][i + 1]
                    df['error'][i] = df['error'][i + 1]
                    df['direction'][i] = df['direction'][i + 1]
                else:
                    df['expected_barcode'][i] = df['expected_barcode'][i]
                    df['error_flag'][i] = df['error_flag'][i]
                    df['error'][i] = df['error'][i]
                    df['direction'][i] = df['direction'][i]
            else:
                pass
        return df

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

    def merge_rows1(self, df):
        for i in df.index:
            if i >= 0 and i != df.index[-1]:
                if (df['butler_id'][i] == df['butler_id'][i + 1] and not pd.isna(df['Time_Diff_rows'][i]) and
                        df['Time_Diff_rows'][i] <= 15):
                    df['error'][i] = df['error'][i + 1]
                else:
                    df['error'][i] = df['error'][i]
            else:
                pass
        return df

    def merge_rows2(self, df):
        for i in df.index:
            if i > 0:
                if (df['butler_id'][i] == df['butler_id'][i - 1] and (
                        pd.isna(df['Time_Diff_rows'][i]) or df['Time_Diff_rows'][i] <= 15)):
                    df['time_start'][i] = df['time_start'][i - 1]
                    df['error_start_time'][i] = df['error_start_time'][i - 1]
            else:
                df['time_start'][i] = df['time_start'][i]
                df['error_start_time'][i] = df['error_start_time'][i]
        return df

    def data_formatting(self, fieldname, df):
        df[fieldname] = df[fieldname].replace(" ", "\ ")
        df[fieldname] = df[fieldname].replace(".", "\.")
        df[fieldname] = df[fieldname].replace("=", "\=")
        return df

    def butler_nav_incidents(self, tenant_info, **kwargs):
        # self.gs_client = storage.Client.from_service_account_json('path/to/your/service-account-file.json')
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.utilfunction = CommonFunction()
        self.gcs = GCS()
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("butler_nav_incidents",self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from butler_nav_error where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.butler_nav_incidents1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("butler_nav_incidents", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.butler_nav_incidents1(self.end_date, **kwargs)
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

                if x_value > 0 and (index - x_value) >= 0:
                    extracted_row = df.iloc[index - x_value]
                    df.at[index, 'L1'] = extracted_row['L1']
                    df.at[index, 'L2'] = extracted_row['L2']

        return df

    def butler_nav_incidents1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("butler_nav_incidents", self.tenant_info)
        # self.start_date_yesterday = pd.to_datetime(self.start_date) - timedelta(hours=24)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0, microsecond=0)
        # self.start_date_yesterday = self.start_date_yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')
        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.start_date = pd.to_datetime(self.start_date).strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        # self.start_date = '2022-11-10 00:00:00'
        # self.start_date_yesterday = '2022-11-09 00:00:00'
        # self.end_date = '2022-11-11 00:00:00'

        self.read_influxclient = self.read_client.influx_client
        if 'HNMCANADA' in self.tenant_info["Name"].upper():
            qry1 = f"select * from butler_nav_error where time >= '{self.start_date}' and time < '{self.end_date}'  AND (error_flag !~ /E_SAFE_CONTROL/   and error_flag !~ /E_RANGER_IN_PAUSED/  AND error_flag !~ /E_TIMEOUT/) and host = '{self.tenant_info['host_name']}'  order by time desc"
        else:
            qry1 = f"select * from butler_nav_error where time >= '{self.start_date}' and time < '{self.end_date}' and host = '{self.tenant_info['host_name']}'  order by time desc"

        print(qry1)
        qry2 = f"select * from butler_events  where  time >= '{self.start_date}' and time < '{self.end_date}'  and host = '{self.tenant_info['host_name']}'  order by time desc"
        print(qry2)
        qry3 = "SHOW FIELD KEYS FROM  butler_nav_incidents "
        df2 = pd.DataFrame(self.client.query(qry3).get_points())
        direction_type = "float"
        if not df2.empty:
            direction_type = df2[df2['fieldKey'] == 'direction']
            if not direction_type.empty:
                direction_type = direction_type.reset_index()
                if direction_type.shape[0] > 1:
                    direction_type = ""
                else:
                    direction_type = direction_type['fieldType'][0]
            else:
                direction_type = "float"

        # ---------------------------------------------------------------------------------------------------------------------------
        bna = self.read_client.fetch_data(self.read_influxclient, qry1)
        bna = pd.DataFrame(bna)
        flag = False
        if 'query' in bna.columns:
            if 'No data fetched' in bna['error'][0]:
                flag = True

        be = self.read_client.fetch_data(self.read_influxclient, qry2)
        be = pd.DataFrame(be)
        if 'query' in be.columns:
            if 'No data fetched' in be['error'][0]:
                flag = True

        if 'name' in be.columns:
            be = be[be['name'] == 'process_message_init']

        if not flag and not be.empty:
            bna['time'] = pd.to_datetime(bna['time'])
            bna['time'] = bna['time'].apply(lambda x: x.to_pydatetime())

            if 'name' not in be:
                be['name'] = 'process_message_init'
            be = be[(be['name'] == 'process_message_init')]
            be['time'] = pd.to_datetime(be['time'])
            be['time'] = be['time'].apply(lambda x: x.to_pydatetime())
            be.drop(['value'], axis=1, inplace=True)
            # be["time2"] = be["time"].dt.round("S")
            bna['expected_barcode'] = bna['expected_barcode'].astype('string')
            bna['expected_barcode'] = bna.apply(
                lambda x: x['old_barcode'] if pd.isna(x['expected_barcode']) or 'notfound' in x[
                    'expected_barcode'] or 'undefined' in x[
                                                  'expected_barcode'] else x[
                    'expected_barcode'], axis=1)
            bna['StartDateTime'] = self.start_date
            bna['EndDateTime'] = self.end_date
            path = f'butler_nav_incidents/{self.tenant_info["Name"]}_carryfile.csv'
            path = os.path.join(CONFFILES_DIR, path)
            source_blob_name = f'{self.tenant_info["Name"]}_carryfile.csv'
            self.gcs.download_from_bucket(GOOGLE_BUCKET_NAME, source_blob_name, path)
            # path ="/home/ankush.j/Downloads/Carryover-butler_nav_incidents_v2_xxx.csv"
            isExist = os.path.isfile(path)
            connection_string = MongoDbServer
            database_name = "GreyOrange"
            collection_name = source_blob_name.replace(".csv", '')
            self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
            data = self.cls_mdb.get_two_days_data(self.start_date)
            if data:
                cof = pd.DataFrame(data)
                if '_id' in cof.columns:
                    del cof['_id']
            elif isExist  and os.path.getsize(path) > 0:
                cof = pd.read_csv(path)
            else:
                cof = pd.DataFrame(
                    columns=['StartDateTime', 'EndDateTime', 'time', 'barcode_length', 'butler_id', 'butler_version',
                             'cstep_coordinate', 'direction', 'error', 'error_flag', 'expected_barcode',
                             'expected_coordinate', 'expected_coordinate_attribute', 'installation_id',
                             'old_barcode', 'old_coordinate', 'old_coordinate_attribute', 'host'])
            if not cof.empty:
                cof["flagtest"] = cof['time'].apply(lambda x: not pd.isna(x))
                cof = cof[cof["flagtest"]]
                if not cof.empty:
                    cof['flagtest'] = cof.time.apply(lambda x: 1 if type(x) == np.float_ else 0)
                if not cof.empty:
                    flag = cof['flagtest'][0]
                if flag == 1:
                    cof['time'] = cof['time'].astype(np.int64) // 10 ** 9
                    cof['time'] = cof['time'].apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x)))
                del cof['flagtest']
                cof['time'] = pd.to_datetime(cof['time'], utc=True)
                # cof['time'] = cof.time.apply(lambda x:pd.Timestamp(x, tz="UTC"))
                # cof['time'] = cof['time'].apply(lambda x: x.to_pydatetime())
                # cof["time2"] = cof["time"].dt.round("S")
                cof["direction"] = cof["direction"].astype('string')
                cof['expected_barcode'] = cof['expected_barcode'].astype('string')
                cof["expected_barcode"] = cof["expected_barcode"].apply(lambda x: x.replace('dummy', ''))

            # bna["time2"] = bna["time"].dt.round("S")
            bna["direction"] = bna["direction"].astype('string')
            bna = pd.concat([bna, cof])
            bna['direction'] = bna['direction'].replace('not_found', '-1')
            bna['expected_barcode'] = bna['expected_barcode'].astype('string')
            bna['old_barcode'] = bna['old_barcode'].astype('string')
            bna['expected_barcode'] = bna.apply(
                lambda x: x['old_barcode'] if pd.isna(x['expected_barcode']) or 'notfound' in x[
                    'expected_barcode'] or 'undefined' in x[
                                                  'expected_barcode'] else x[
                    'expected_barcode'], axis=1)

            bna.sort_values(by=['time'], inplace=True)
            bna = bna.drop_duplicates(subset=['installation_id', 'time', 'butler_id'], keep='first')
            # to be used to make join to create carryover file
            # bna = bna.reset_index()
            bna = self.utilfunction.reset_index(bna)
            bna['butler_id'] = bna['butler_id'].astype(int)
            be['butler_id'] = be['butler_id'].astype(int)
            df1 = bna.copy()
            bna = pd.merge(bna, be[['time', 'butler_id']], on='butler_id')
            # check time
            # bna.rename(
            #     columns={'time_x': 'time_start', 'time_y': 'time_end', 'time_x': 'error_start_time',
            #              'time_y': 'Error_End_Time'},
            #     inplace=True)
            bna.rename(
                columns={'time_x': 'Error_Start_Time_NS', 'time_y': 'Error_End_Time'},
                inplace=True)
            bna['time_start'] = bna['Error_Start_Time_NS']
            bna['time_end'] = bna['Error_End_Time']
            bna['error_start_time'] = bna['Error_Start_Time_NS']

            bna = bna[bna['time_start'] <= bna['time_end']]
            bna = bna.sort_values(['installation_id', 'butler_id', 'time_start', 'time_end'])
            bna['time'] = bna['time_start']
            df1['flag'] = 'left'
            bna['flagg'] = 'right'
            # save_in_cof = sqldf("select a.*  from df1 a left join bna b on a.butler_id =b.butler_id and a.time= b.time_start where b.butler_id is null")
            save_in_cof = pd.merge(df1, bna[['butler_id', 'time', 'flagg']], on=['butler_id', 'time'], how='left')
            if not save_in_cof.empty:
                save_in_cof = save_in_cof[save_in_cof['flagg'] != 'right']
                save_in_cof["expected_barcode"] = save_in_cof["expected_barcode"].apply(lambda x: "dummy" + str(x))
                del save_in_cof['flagg']
            # remove extra columns and save this in csv
            if bna.empty:
                return
            bni = bna.drop_duplicates(subset=['installation_id', 'butler_id', 'error_start_time'], keep='first')
            bni.drop(['flagg'], axis=1, inplace=True)
            bni = bni.sort_values(['installation_id', 'butler_id', 'time_end', 'time_start'])
            bni['flag'] = 9
            bni = self.utilfunction.reset_index(bni)
            bni = self.put_flag(bni)
            bni = bni[bni['flag'] == 0]
            bni = bni.sort_values(['installation_id', 'butler_id', 'time_end', 'time_start'])
            bni = self.utilfunction.reset_index(bni)
            bni = self.alter_flag(bni)
            bni.sort_values(by=['installation_id', 'butler_id', 'time_end', 'time_start'], inplace=True)
            bni = self.utilfunction.reset_index(bni)
            bni = bni.drop_duplicates(subset=['installation_id', 'butler_id', 'Error_End_Time'], keep='first')
            bni['time_difference'] = None
            bni['Time_Diff_rows'] = None
            bni = self.utilfunction.reset_index(bni)
            bni = self.utilfunction.datediff_in_sec(bni, 'Error_End_Time', 'error_start_time', 'time_difference')
            bni = self.utilfunction.reset_index(bni)
            bni = self.time_diff_rows(bni)
            bni = self.merge_rows1(bni)
            bni = self.merge_rows2(bni)
            if 'level_0' in bni.columns:
                del bni['level_0']

            bni = self.utilfunction.reset_index(bni)
            bni = bni.drop_duplicates(subset=['installation_id', 'butler_id', 'error_start_time'], keep='first')
            if bni.empty:
                # save_in_cof.to_csv(path, index=False, header=True)
                # self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
                if self.cls_mdb.delete_data({}, multiple=True):
                    if not save_in_cof.empty:
                        json_list = save_in_cof.to_json(orient="records")
                        json_list = json.loads(json_list)
                        test = self.cls_mdb.insert_data(json_list)
                    else:
                        save_in_cof.to_csv(path, index=False, header=True)
                        self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)

                    #print(test)
                return
            bni = self.data_formatting('error', bni)
            bni = self.data_formatting('error_flag', bni)
            bni = self.data_formatting('expected_barcode', bni)
            #####################################3
            # read google sheet data
            # SHEET_ID = '1zxuoKDL8VG51B-8DLTK8TK4eY3A6VEQoaEdP1gK9vMo'
            # SHEET_NAME = 'Error Flag Details'
            # gc = gspread.service_account(GOOGLE_AUTH_JSON)
            # spreadsheet = gc.open_by_key(SHEET_ID)
            # worksheet = spreadsheet.worksheet(SHEET_NAME)
            # rows = worksheet.get_all_values()
            # df_sheet = pd.DataFrame(rows)
            # expected_headers = df_sheet.iloc[1]
            # df_sheet.columns = expected_headers
            df_sheet = self.utilfunction.get_error_data_flag_sheet()
            df_sheet = df_sheet[(df_sheet['L1'] != '')]
            df_sheet = df_sheet[(pd.isna(df_sheet['L1']) == False)]
            df_sheet["checkflag"] = df_sheet['Is_auto_resolved'].apply(lambda x: 1 if x == "Yes" or x == "*" else 0)
            df_sheet = df_sheet[(df_sheet['checkflag'] == 0)]
            del df_sheet["checkflag"]
            if 'Is_auto_resolved' in bni.columns:
                del bni['Is_auto_resolved']

            if 'L1' in bni.columns:
                del bni['L1']

            if 'L2' in bni.columns:
                del bni['L2']
            bni = pd.merge(bni, df_sheet, left_on=['error_flag'], right_on=['Error Flag'], how='left')
            # ####################################333
            bni['time_difference'] = bni['time_difference'].fillna(0)
            bni['Is_auto_resolved'] = bni.apply(
                lambda x: "Yes" if (x['time_difference'] < 20 and x['error_flag'] == '{0,0,0,8,0,0,0,0}') else x[
                    'Is_auto_resolved'], axis=1)
            bni['L1'] = bni['L1'].apply(lambda x: "null" if pd.isna(x) or len(x) == 0 else x)
            bni['L2'] = bni['L2'].apply(lambda x: "null" if pd.isna(x) or len(x) == 0 else x)
            bni['Is_auto_resolved'] = bni['Is_auto_resolved'].apply(
                lambda x: "Yes" if pd.isna(x) or '*' in x or x == "" else x)
            # # ############################################333
            # # #fetch data from grid barcode mapping
            # bni = self.correct_manual_interventions(bni)
            self.read_client1 = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                           db="Alteryx")
            self.read_influxclient1 = self.read_client1.influx_client
            q = f"select cron_ran_at from rp_seven_days.grid_barcode_mapping where  time >now()-30d  and host = '{self.tenant_info['host_name']}' order by time desc limit 1"
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

                bni['expected_barcode'] = bni['expected_barcode'].apply(
                    lambda x: x.replace('{', '[') if not pd.isna(x) else x)
                bni['expected_barcode'] = bni['expected_barcode'].apply(
                    lambda x: x.replace('}', ']') if not pd.isna(x) else x)
                if not df_gridbarcode.empty:
                    bni1 = pd.merge(bni, df_gridbarcode, left_on=['expected_barcode'], right_on=['barcode'],
                                    how='inner')
                    if bni1.empty:
                        bni = pd.merge(bni, df_gridbarcode, left_on=['expected_barcode'], right_on=['coordinate'],
                                       how='left')
                    else:
                        bni = pd.merge(bni, df_gridbarcode, left_on=['expected_barcode'], right_on=['barcode'],
                                       how='left')

                    bni['barcode'] = bni['barcode'].apply(lambda x: None if pd.isna(x) else x)
                    bni['barcode_group'] = bni['category'].apply(lambda x: None if pd.isna(x) else x)
                    bni['zone'] = bni['zone'].apply(lambda x: None if pd.isna(x) else x)

                l = ['level_0', 'index', 'expected_coordinate', 'host', \
                     'old_barcode', 'old_coordinate', 'value', 'StartDateTime', 'EndDateTime', 'barcode_length',
                     'butler_version', 'cstep_coordinate', 'expected_coordinate_attribute',
                     'old_coordinate_attribute', 'flag', \
                     'Error_End_Time', 'Time_Diff_rows', 'Error Flag', 'Error Flag Definition', 'Error Categories',
                     'Considered as Stop?', 'Error group', 'Hardware Error', 'category', '', 'coordinate',
                     'error_start_time', 'Error_Start_Time_NS']

                for col in l:
                    if col in bni.columns:
                        del bni[col]

                bni.rename(columns={'error': 'error_name', 'time_start': 'start_time', 'time_end': 'end_time',
                                    'time_x': 'time'},
                           inplace=True)

                bni.drop(['time_y'], axis=1, inplace=True)
                ##############################################333
                bni.time = pd.to_datetime(bni.time)
                bni.start_time = bni.start_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
                bni.end_time = bni.end_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
                bni['time_difference'] = bni['time_difference'].astype(float)
                bni['Is_auto_resolved'] = bni['Is_auto_resolved'].astype(str)
                bni['L1'] = bni['L1'].astype(str)
                bni['L2'] = bni['L2'].astype(str)
                bni['barcode'] = bni['barcode'].astype(str)
                bni['barcode_group'] = bni['barcode_group'].astype(str)
                bni['butler_id'] = bni['butler_id'].astype(str)
                bni['end_time'] = bni['end_time'].astype(str)
                bni['error_flag'] = bni['error_flag'].astype(str)
                bni['error_name'] = bni['error_name'].astype(str)
                bni['error_category'] = bni['error_name'].str.split('|', expand=True)[0]
                bni['expected_barcode'] = bni['expected_barcode'].astype(str)
                bni['start_time'] = bni['start_time'].astype(str)
                bni['zone'] = bni['zone'].astype(str)
                bni["direction_int"] = bni['direction'].apply(lambda x: int(x) if str(x).isdigit() else 0)
                bni["direction_str"] = bni['direction'].astype(str)
                bni["direction_int"] = bni['direction_int'].astype(float)
                if direction_type == 'float':
                    bni["direction"] = bni['direction'].astype(float)
                elif direction_type == 'string':
                    bni["direction"] = bni['direction'].astype(str)
                else:
                    bni = bni.drop(columns=['direction'])
                #bni = bni.set_index('time')
                # bni.to_csv(r'/home/ankush.j/Desktop/workname/big_data/bni.csv', index=False, header=True)
                # save this data in carry over files(only unique data)
                # save_in_cof.to_csv(path, index=False, header=True)
                # self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME,path,source_blob_name)
                if self.cls_mdb.delete_data({}, multiple=True):
                    if not save_in_cof.empty:
                        save_in_cof['time'] = save_in_cof['time'].apply(lambda x: self.utilfunction.convert_to_nanosec(x))
                        json_list = save_in_cof.to_json(orient="records")
                        json_list = json.loads(json_list)
                        test = self.cls_mdb.insert_data(json_list)
                    else:
                        save_in_cof.to_csv(path, index=False, header=True)
                        self.gcs.upload_to_bucket(GOOGLE_BUCKET_NAME, path, source_blob_name)
                    #print(test)
                if 'Unnamed: 9' in bni.columns:
                    del bni['Unnamed: 9']
                if 'Unnamed: 10' in bni.columns:
                    del bni['Unnamed: 10']
                bni.time = pd.to_datetime(bni.time)
                bni = bni.set_index('time')
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                     port=self.tenant_info["write_influx_port"])
                self.write_client.writepoints(bni, "butler_nav_incidents", db_name=self.tenant_info["out_db_name"],
                                              tag_columns=['butler_id', 'error_name'],
                                              dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        else:
            filter = {"site": self.tenant_info['Name'], "table": "butler_nav_incidents"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)
        return None


# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
        'butler_nav_incidents',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of Orderline transactions',
        schedule_interval='20 * * * *',
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
            if tenant['Active'] == "Y" and tenant['butler_nav_incidents'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                butler_nav_incidents_final_task = PythonOperator(
                    task_id='butler_nav_incidents_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(butler_nav_incidents().butler_nav_incidents,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        butler_nav_incidents_final_task = PythonOperator(
            task_id='butler_nav_incidents_final',
            provide_context=True,
            python_callable=functools.partial(butler_nav_incidents().butler_nav_incidents,
                                              tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )
