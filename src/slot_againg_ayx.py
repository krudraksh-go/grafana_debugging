
## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, postgres_connection, CommonFunction
import numpy as np
from slot_utilization_ayx import slot_utilzation
import pytz
from config import (
    rp_seven_days
)
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')
Postgres_ButlerDev = os.environ.get('Postgres_ButlerDev', Butler_ip)
Postgres_butler_port = os.environ.get('Postgres_butler_port', 5432)
Postgres_butler_password = os.environ.get('Postgres_butler_password', "YLafmbfuji$@#45!")
Postgres_butler_user = os.environ.get('Postgres_butler_user', "altryx_read")
Tower_password = os.environ.get('Tower_password', "YLafmbfuji$@#45!")
Tower_user = os.environ.get('Tower_password', "altryx_read")
Postgres_tower_port = os.environ.get('Postgres_tower_port', 5432)
Postgres_tower = os.environ.get('Postgres_tower', "localhost")
streaming = os.environ.get('streaming', "N")
TimeZone = os.environ.get('TimeZone', 'PDT')
sslrootcert = os.environ.get('sslrootcert', "")
sslcert = os.environ.get('sslcert', "")
sslkey = os.environ.get('sslkey', "")


# dag = DAG(
#     'slot_againg',
#     default_args = default_args,
#     description = 'calculation of puts per rack face GM-44025',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class slot_againg:
    def divide_slotref(self, df):
        df['rack_id'] = df['slot'].str.split('.', expand=True)[0]
        df['rack_face'] = df['slot'].str.split('.', expand=True)[1]
        # df['slot'] = df['slot_id'].str.split('.', 2, expand=True)[2]
        return df

    def get_season(self, df):
        df['season'] = df['season'].apply(lambda x: x.replace(x[-3:], '') if len(x) >= 3 else '')
        df['season'] = df['season'].apply(lambda x: x[-3:] if len(x) >= 3 else x)
        return df

    def check_if_null(self, a, b):
        if a.isna:
            val = b
        else:
            val = a
        return int(val)

    def df_cleanup(self, df):
        df = df.drop(columns=['tpid_y', 'item_id_y', 'time_y', 'rack_id_y'])
        df = df.rename(
            columns={'time_x': 'time', 'item_id_x': 'item_id', 'rack_id_x': 'rack_id', 'tpid_x': 'tpid'})
        return df

    def calc_untouched_ageing(self, df):
        if not df.empty:
            df["latest_inventory_transaction"] = pd.to_datetime(df["latest_inventory_transaction"], utc=True)
            df["last_transaction_time"] = pd.to_datetime(df["last_transaction_time"], utc=True)
            df = self.utilfunction.datediff_in_sec(df, "latest_inventory_transaction", "last_transaction_time",
                                                   "untouched_ageing")
            df['untouched_ageing'] = df['untouched_ageing'] / 3600
        return df

    def calc_untouched_ageing2(self, df):
        if not df.empty:
            df["latest_inventory_transaction"] = pd.to_datetime(df["latest_inventory_transaction"], utc=True)
            df["transaction_time"] = pd.to_datetime(df["transaction_time"], utc=True)
            df = self.utilfunction.datediff_in_sec(df, "latest_inventory_transaction", "transaction_time",
                                                   "untouched_ageing")
            df['untouched_ageing'] = df['untouched_ageing'] / 3600
        return df

    def calc_slot_ageing(self, df):
        if not df.empty:
            df["latest_inventory_transaction"] = pd.to_datetime(df["latest_inventory_transaction"], utc=True)
            df["first_put_time"] = pd.to_datetime(df["first_put_time"], utc=True)
            df = self.utilfunction.datediff_in_sec(df, "latest_inventory_transaction", "first_put_time", "slot_ageing")
            df['slot_ageing'] = df['slot_ageing'] / 3600
        return df

    def calc_sku_ageing(self, df):
        if not df.empty:
            df["first_cron_ran_at"] = pd.to_datetime(df["first_cron_ran_at"], utc=True)
            df["Min_First_first_put_time"] = pd.to_datetime(df["Min_First_first_put_time"], utc=True)
            df = self.utilfunction.datediff_in_sec(df, "first_cron_ran_at", "Min_First_first_put_time", "sku_ageing")
            df['sku_ageing'] = df['sku_ageing'] / 3600
        return df

    def fetch_tote_info_from_tower(self):
        try:
            self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                     sslrootcert=self.tenant_info['sslrootcert'],
                                                     sslcert=self.tenant_info['sslcert'], \
                                                     sslkey=self.tenant_info['sslkey'],
                                                     host=self.tenant_info['Postgres_tower'], \
                                                     port=self.tenant_info['Postgres_tower_port'],
                                                     password=self.tenant_info['Tower_password'],
                                                     dag_name=os.path.basename(__file__),
                                                     site_name=self.tenant_info['Name'])
            totalcount = self.postgres_conn.fetch_postgres_data_in_chunk("select count(*) from data_tote")
            self.postgres_conn.close()
            if totalcount[0][0] == 0:
                tote_flow = True

            f = True
            time_offset = 0
            l = 2000
            print(f"total rows:{totalcount[0][0]}")
            while f == True:
                if time_offset <= totalcount[0][0]:
                    self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                             sslrootcert=self.tenant_info['sslrootcert'],
                                                             sslcert=self.tenant_info['sslcert'], \
                                                             sslkey=self.tenant_info['sslkey'],
                                                             host=self.tenant_info['Postgres_tower'], \
                                                             port=self.tenant_info['Postgres_tower_port'],
                                                             password=self.tenant_info['Tower_password'],
                                                             dag_name=os.path.basename(__file__),
                                                             site_name=self.tenant_info['Name'])
                    qry = "select tote_id, tote_type from data_tote order by tote_id offset "
                    qry = qry + str(time_offset) + " limit " + str(l)
                    print(qry)
                    temp_data = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
                    self.postgres_conn.close()
                    temp_data = pd.DataFrame(temp_data, columns=["rack_id", "rack_type"])
                    if time_offset == 0:
                        tower_rack = temp_data
                    else:
                        tower_rack = pd.concat([tower_rack, temp_data])
                    time_offset = time_offset + l
                else:
                    f = False
            return tower_rack
        except:
            return pd.DataFrame(columns=["rack_id", "rack_type"])

    def fetch_rack_info_from_tower(self):
        self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                 sslrootcert=self.tenant_info['sslrootcert'],
                                                 sslcert=self.tenant_info['sslcert'], \
                                                 sslkey=self.tenant_info['sslkey'],
                                                 host=self.tenant_info['Postgres_tower'], \
                                                 port=self.tenant_info['Postgres_tower_port'],
                                                 password=self.tenant_info['Tower_password'],
                                                 dag_name=os.path.basename(__file__),
                                                 site_name=self.tenant_info['Name'])
        totalcount = self.postgres_conn.fetch_postgres_data_in_chunk("select count(*) from data_rack")
        self.postgres_conn.close()

        f = True
        time_offset = 0
        l = 2000
        print(f"total rows:{totalcount[0][0]}")
        while f == True:
            if time_offset <= totalcount[0][0]:
                self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                         sslrootcert=self.tenant_info['sslrootcert'],
                                                         sslcert=self.tenant_info['sslcert'], \
                                                         sslkey=self.tenant_info['sslkey'],
                                                         host=self.tenant_info['Postgres_tower'], \
                                                         port=self.tenant_info['Postgres_tower_port'],
                                                         password=self.tenant_info['Tower_password'],
                                                         dag_name=os.path.basename(__file__),
                                                         site_name=self.tenant_info['Name'])
                qry = "select rack_id, rack_type from data_rack order by rack_id offset "
                qry = qry + str(time_offset) + " limit " + str(l)
                print(qry)
                temp_data = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
                self.postgres_conn.close()
                temp_data = pd.DataFrame(temp_data, columns=["rack_id", "rack_type"])
                if time_offset == 0:
                    tower_rack = temp_data
                else:
                    tower_rack = pd.concat([tower_rack, temp_data])
                time_offset = time_offset + l
            else:
                f = False
        try:
            tote_df = self.fetch_tote_info_from_tower()
            if not tote_df.empty:
                tower_rack = pd.concat([tower_rack, tote_df])
        except:
            pass
        return tower_rack

    def slot_againg_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.utilfunction = CommonFunction()
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["out_db_name"], timeout=600)
        #        self.client=InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        if self.client.is_table_present(f'{rp_seven_days}.slots_ageing_ayx_latest'):
            table_name = f'{rp_seven_days}.slots_ageing_ayx_latest'
        elif self.client.is_table_present('slots_ageing_ayx_latest'):
            table_name = 'slots_ageing_ayx_latest'
        else:
            table_name = 'slots_ageing_ayx'

        q = f"select * from {table_name} where time > now()-1d order by time desc limit 1"
        cron_run_at = pd.DataFrame(self.client.query(q).get_points())
        if cron_run_at.empty:
            q = f"select * from {table_name} where time > now()-7d order by time desc limit 1"
            cron_run_at = pd.DataFrame(self.client.query(q).get_points())

        last_transaction_time = cron_run_at["last_transaction_time"][0]
        latest_inventory_transaction = pd.to_datetime(cron_run_at["latest_inventory_transaction"][0])
        self.start_date = cron_run_at["cron_ran_at"][0]
        current_transaction_time = cron_run_at['time'][0]
        self.end_date = datetime.now(timezone.utc)
        self.end_date = self.end_date.replace(second=0)

        # self.start_date = self.client.get_start_date("slots_utilization_ayx")
        # self.end_date = datetime.now(timezone.utc)
        # self.end_date = self.end_date.replace(minute=0,second=0)
        # self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
        #
        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        rackinfo = self.fetch_rack_info_from_tower()
        q_cnt = f"select count(cron_ran_at) from {table_name} where time >= '{self.start_date}' and time<= '{self.end_date}' and remaining_uom>0 order by time desc"
        temp_slotdata = pd.DataFrame(self.client.query(q_cnt).get_points())
        q_cnt = temp_slotdata['count'][0]
        offset_val = 0
        data_limit = 5000
        while offset_val < q_cnt:
            q = f"select * from {table_name} where time >= '{self.start_date}' and time<= '{self.end_date}' and remaining_uom>0 order by time desc limit {data_limit} offset {offset_val}"
            temp_slotdata = pd.DataFrame(self.client.query(q).get_points())
            if offset_val == 0:
                df_prev = temp_slotdata
            else:
                df_prev = pd.concat([df_prev, temp_slotdata])
            offset_val = offset_val + data_limit

        if not df_prev.empty:
            l = ['brand', 'latest_inventory_transaction', 'season', 'sku_ageing', 'sku_id', 'sku_touched_untouched',
                 'station_type_x', 'station_type_y', 'storage_type_x', 'storage_type_y']
            for col in l:
                if col in df_prev.columns:
                    del df_prev[col]
            # group_df = df_prev.groupby(['slot_id'], as_index=False).agg(first_put_time=('first_put_time', 'max'))
            # df_prev = pd.merge(df_prev, group_df, on=['slot_id', 'first_put_time'], how='inner')

        columnlist = ['ID', 'transaction_id', 'external_service_request_id', 'transaction_type',
                      'request_id', 'event_name', 'user_name', 'slot', 'tpid', 'item_id',
                      'old_uom', 'delta_uom', 'new_uom', 'process_id', 'time', 'pps_id',
                      'ppsbin_id', 'unique_id', 'start_date_time', 'end_date_time', 'rack_id', 'rack_face']
        if self.tenant_info['is_ttp_setup'] == 'Y':
            columnlist2 = ['ID', 'transaction_id', 'external_service_request_id', 'transaction_type',
                           'request_id', 'event_name', 'user_name', 'slot', 'tpid', 'item_id',
                           'old_uom', 'delta_uom', 'new_uom', 'process_id', 'time', 'pps_id',
                           'ppsbin_id', 'station_type', 'storage_type']
        else:
            columnlist2 = ['ID', 'transaction_id', 'external_service_request_id', 'transaction_type',
                           'request_id', 'event_name', 'user_name', 'slot', 'tpid', 'item_id',
                           'old_uom', 'delta_uom', 'new_uom', 'process_id', 'time', 'pps_id',
                           'ppsbin_id']


        temp = cron_run_at['latest_inventory_transaction'][0]
        if self.tenant_info['Postgres_butler_user'] != "":

            qry = f"select count(*) from inventory_transaction_archives \
                     where time>= '{temp}'"

            self.postgres_conn = postgres_connection(database='butler_dev',
                                                     user=self.tenant_info['Postgres_butler_user'], \
                                                     sslrootcert=self.tenant_info['sslrootcert'],
                                                     sslcert=self.tenant_info['sslcert'], \
                                                     sslkey=self.tenant_info['sslkey'],
                                                     host=self.tenant_info['Postgres_ButlerDev'], \
                                                     port=self.tenant_info['Postgres_butler_port'],
                                                     password=self.tenant_info['Postgres_butler_password'],
                                                     dag_name=os.path.basename(__file__),
                                                     site_name=self.tenant_info['Name'])

            totalcount = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
            self.postgres_conn.close()
            f = True
            time_offset = 0
            l = 10000
            print(f"total orders:{totalcount[0][0]}")
            tempcount = 100000
            if totalcount[0][0] < tempcount:
                tempcount = totalcount[0][0]

            while f == True:
                if time_offset <= tempcount:
                    if self.tenant_info['is_ttp_setup'] == 'Y':
                        qry = f"select id,transaction_id,external_service_request_id,transaction_type,\
                        request_id,event_name, user_name, slot, tpid, item_id, old_uom, delta_uom, new_uom, \
                        process_id, time, pps_id , ppsbin_id,station_type,storage_type from inventory_transaction_archives \
                        where time>= '{temp}' order by time  offset "
                    else:
                        qry = f"select id,transaction_id,external_service_request_id,transaction_type,\
                        request_id,event_name, user_name, slot, tpid, item_id, old_uom, delta_uom, new_uom, \
                        process_id, time, pps_id , ppsbin_id from inventory_transaction_archives \
                        where time>= '{temp}' order by time  offset "
                    qry = qry + str(time_offset) + " limit " + str(l)
                    print(qry)
                    self.postgres_conn = postgres_connection(database='butler_dev',
                                                             user=self.tenant_info['Postgres_butler_user'], \
                                                             sslrootcert=self.tenant_info['sslrootcert'],
                                                             sslcert=self.tenant_info['sslcert'], \
                                                             sslkey=self.tenant_info['sslkey'],
                                                             host=self.tenant_info['Postgres_ButlerDev'], \
                                                             port=self.tenant_info['Postgres_butler_port'],
                                                             password=self.tenant_info['Postgres_butler_password'],
                                                             dag_name=os.path.basename(__file__),
                                                             site_name=self.tenant_info['Name'])

                    Orders = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
                    self.postgres_conn.close()
                    tempdf = pd.DataFrame(Orders, columns=columnlist2)
                    tempdf = tempdf.loc[
                        (tempdf['transaction_type'] == 'pick') | (tempdf['transaction_type'] == 'put') | (
                                tempdf['transaction_type'] == 'audit')]
                    if time_offset == 0:
                        df = tempdf
                    else:
                        df = pd.concat([df, tempdf])
                    time_offset = time_offset + l
                else:
                    f = False

            if df.empty:
                return None
            df = self.utilfunction.reset_index(df)
            if self.tenant_info['is_ttp_setup'] == 'Y':
                df['station_type'] = df.apply(lambda x: 'rtp' if pd.isna(x['station_type']) else x['station_type'],
                                              axis=1)
                df['storage_type'] = df.apply(lambda x: 'msu' if pd.isna(x['storage_type']) else x['storage_type'],
                                              axis=1)

                if 'station_type' not in df_prev.columns:
                    df_prev['station_type'] = 'rtp'
                    df_prev['storage_type'] = 'msu'
            else:
                df['station_type'] = 'rtp'
                df['storage_type'] = 'msu'

                df_prev['station_type'] = 'rtp'
                df_prev['storage_type'] = 'msu'
            current_transaction_time = df['time'].iloc[-1]
            df_prev['flag'] = df_prev['flag'].apply(lambda x: 'touched' if x=='Touched' else x)

            df = self.divide_slotref(df)
            df['old_uom'] = df.old_uom.astype(str).str.extract('(\d+)}$', expand=False)
            df['new_uom'] = df.new_uom.astype(str).str.extract('(\d+)}$', expand=False)
            df['delta_uom'] = df.delta_uom.astype(str).str.extract('(\d+)}$', expand=False)
            df['item_id'] = df['item_id'].astype(str)
            df['unique_id'] = df['item_id'] + '_' + df['slot']
            df['start_date_time'] = self.start_date
            df['end_date_time'] = self.end_date
        else:
            df = pd.DataFrame(columns=columnlist)
        df['old_uom'] = df['old_uom'].fillna(0).astype(int)
        df['new_uom'] = df['new_uom'].fillna(0).astype(int)
        df['delta_uom'] = df['delta_uom'].fillna(0).astype(int)
        current_transaction_time = pd.to_datetime(current_transaction_time)
        latest_inventory_transaction = pd.to_datetime(latest_inventory_transaction)
        current_transaction_time = current_transaction_time.strftime('%Y-%m-%d %H:%M:%S')
        latest_inventory_transaction = latest_inventory_transaction.strftime('%Y-%m-%d %H:%M:%S')
        if self.utilfunction.value_in_sec(current_transaction_time, latest_inventory_transaction) > 0:
            latest_inventory_transaction = current_transaction_time

        # left not matched data(alteryx right join)--first record
        df['latest_inventory_transaction'] = pd.to_datetime(latest_inventory_transaction, utc=True)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        last_pick_df= df.groupby(['unique_id','transaction_type'], as_index= False).agg(last_event_time_new= ('time','max'))
        df_prev['time'] = pd.to_datetime(df_prev['time'], utc=True)
        df_4b = pd.merge(df_prev, df, how='left', on=['unique_id', 'station_type', 'storage_type'])
        df_4b = df_4b.loc[(pd.isna(df_4b['time_y']) == True)]
        df_4b = self.utilfunction.reset_index(df_4b)
        df_4b['latest_inventory_transaction'] = pd.to_datetime(latest_inventory_transaction, utc=True)
        df_4b['last_transaction_time'] = pd.to_datetime(df_4b['last_transaction_time'], utc=True)
        df_4b['first_put_time'] = pd.to_datetime(df_4b['first_put_time'], utc=True)
        df_4b['untouched_ageing'] = None
        df_4b['slot_ageing'] = None

        df_4b = self.calc_untouched_ageing(df_4b)
        df_4b = self.calc_slot_ageing(df_4b)
        df_4b = self.df_cleanup(df_4b)
        df_4b = df_4b.drop(columns=['ID', 'transaction_id',
                                    'external_service_request_id', 'transaction_type', 'request_id',
                                    'event_name', 'user_name', 'slot', 'old_uom', 'delta_uom', 'new_uom',
                                    'process_id', 'pps_id', 'ppsbin_id', 'rack_face', 'start_date_time',
                                    'end_date_time'])

        # ## now left join for 2nd record
        dfb = pd.merge(df, df_prev, how='left', on=['unique_id', 'station_type', 'storage_type'])  # go through historic
        dfb = dfb.loc[(pd.isna(dfb['time_y']) == True)]
        dfb = self.df_cleanup(dfb)
        dfb['old_uom'] = dfb['old_uom'].astype(int)
        dfb['new_uom'] = dfb['new_uom'].astype(int)
        df_case1 = dfb.loc[(dfb['transaction_type'] == 'put') & (dfb['old_uom'] == 0)]
        df_case1 = df_case1.sort_values(by=['unique_id', 'time'], ascending=[True, False])
        df_case1['RecordID'] = np.arange(1, (df_case1.shape[0] + 1), 1)
        df_unique_pps_installation = df_case1[['unique_id', 'time', 'RecordID']]
        df_unique_pps_installation = df_unique_pps_installation.groupby(['unique_id'], as_index=False).agg(
            time=('time', 'first'), RecordID=('RecordID', 'first'))
        df_case1 = pd.merge(df_case1, df_unique_pps_installation, how='inner',
                            on=['unique_id', 'time', 'RecordID'])
        del df_case1['RecordID']
        dfb = dfb.loc[(dfb['transaction_type'] != 'put') | (dfb['old_uom'] != 0)]
        dfb = dfb.sort_values(by=['unique_id', 'time'], ascending=[True, False])
        dfb['RecordID'] = np.arange(1, (dfb.shape[0] + 1), 1)
        df_unique_pps_installation = dfb[['unique_id', 'time', 'RecordID']]
        df_unique_pps_installation = df_unique_pps_installation.groupby(['unique_id'], as_index=False).agg(
            time=('time', 'first'), RecordID=('RecordID', 'first'))
        dfb = pd.merge(dfb, df_unique_pps_installation, how='inner', on=['unique_id', 'time', 'RecordID'])
        del dfb['RecordID']
        dfb['right_time'] = dfb['time']
        dfb['right_transaction_type'] = dfb['transaction_type']
        dfb['right_new_uom'] = dfb['new_uom']
        dfb['right_rack_id'] = dfb['rack_id']
        dfb['right_rack_face'] = dfb['rack_face']
        dfb['right_slot'] = dfb['slot']
        df_inner = pd.merge(df_case1, dfb[['unique_id', 'right_time', 'right_transaction_type', 'right_new_uom']],
                            how='inner', on='unique_id')
        df_case3 = pd.merge(df_case1, dfb[['unique_id', 'right_time']], how='left', on='unique_id')
        df_case3 = df_case3.loc[(pd.isna(df_case3['right_time']) == True)]
        del df_case3['right_time']
        df_case1['left_time'] = df_case1['time']
        df_case2 = pd.merge(df_case1[['unique_id', 'left_time']], dfb, how='right', on='unique_id')
        df_case2 = df_case2.loc[(pd.isna(df_case2['left_time']) == True)]
        del df_case2['left_time']
        df_case11 = df_inner.loc[(df_inner['time'] >= df_inner['right_time'])]
        df_case12 = df_inner.loc[(df_inner['time'] < df_inner['right_time'])]
        df_case12 = df_case12.drop(
            columns=['ID', 'transaction_id', 'external_service_request_id', 'transaction_type', 'request_id',
                     'event_name', 'user_name', 'old_uom', 'delta_uom', 'process_id', 'pps_id', 'ppsbin_id',
                     'rack_face', 'start_date_time', 'end_date_time', 'cron_ran_at', 'first_put_time',
                     'first_put_uom', 'flag', 'last_transaction_event', 'last_transaction_time', 'remaining_uom',
                     'slot_ageing', 'slot_id',
                     'untouched_ageing'])
        df_case12['first_put_uom'] = df_case12['new_uom']
        df_case12['first_put_time'] = df_case12['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df_case12['transaction_type'] = df_case12['right_transaction_type']
        df_case12['remaining_uom'] = df_case12['right_new_uom']
        df_case12['transaction_time'] = df_case12['right_time']
        df_case12 = df_case12.drop(columns=['new_uom', 'right_transaction_type', 'right_new_uom', 'right_time'])
        df_case12['flag'] = "touched"

        df_case11 = df_case11.drop(columns=['ID', 'transaction_id', 'external_service_request_id',
                                            'request_id', 'event_name', 'user_name',
                                            'old_uom', 'delta_uom', 'process_id',
                                            'pps_id', 'ppsbin_id', 'rack_face',
                                            'start_date_time', 'end_date_time', 'cron_ran_at', 'first_put_time',
                                            'first_put_uom', 'flag', 'last_transaction_event',
                                            'last_transaction_time', 'slot_ageing', 'slot_id',
                                            'untouched_ageing', 'right_time', 'right_transaction_type',
                                            'right_new_uom'
                                            ])
        df_case11['first_put_uom'] = df_case11['new_uom']
        df_case11['first_put_time'] = df_case11['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df_case11['remaining_uom'] = df_case11['first_put_uom']
        df_case11['transaction_time'] = df_case11['time'].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))  ##need to check
        df_case11['flag'] = "untouched"
        df_case11 = df_case11[(df_case11['remaining_uom'] > 0)]

        df_case11 = df_case11.drop(columns=['new_uom'])
        df_case3 = df_case3.drop(columns=['ID', 'transaction_id', 'external_service_request_id',
                                          'request_id', 'event_name', 'user_name',
                                          'old_uom', 'delta_uom', 'process_id',
                                          'pps_id', 'ppsbin_id', 'rack_face',
                                          'start_date_time', 'end_date_time', 'cron_ran_at', 'first_put_time',
                                          'first_put_uom', 'flag', 'last_transaction_event',
                                          'last_transaction_time', 'slot_ageing', 'slot_id',
                                          'untouched_ageing',
                                          ])
        df_case3['first_put_time'] = df_case3['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df_case3['first_put_uom'] = df_case3['new_uom']
        df_case3['remaining_uom'] = df_case3['first_put_uom']
        df_case3['flag'] = "untouched"
        df_case3 = df_case3.drop(columns=['new_uom'])
        df_case3 = df_case3[(df_case3['remaining_uom'] > 0)]
        df_case2['flag'] = "untouched"
        df_case2 = df_case2.drop(columns=['ID', 'transaction_id', 'external_service_request_id',
                                          'request_id', 'event_name', 'user_name',
                                          'old_uom', 'delta_uom', 'process_id',
                                          'pps_id', 'ppsbin_id', 'rack_face',
                                          'start_date_time', 'end_date_time', 'cron_ran_at', 'first_put_time',
                                          'first_put_uom', 'last_transaction_event',
                                          'last_transaction_time', 'slot_ageing', 'slot_id',
                                          'untouched_ageing', 'right_time', 'right_transaction_type',
                                          'right_new_uom', 'right_rack_id', 'right_rack_face', 'right_slot',
                                          ])

        df_case2['first_put_uom'] = df_case2['new_uom']
        df_case2['remaining_uom'] = df_case2['first_put_uom']
        df_case2['first_put_time'] = df_case2['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df_case2['remaining_uom'] = df_case2['remaining_uom'].fillna(0)
        df_case2['remaining_uom'] = df_case2['remaining_uom'].astype('int')
        df_case2 = df_case2[(df_case2['remaining_uom'] > 0)]
        df_case2 = df_case2.drop(columns=['new_uom'])
        df_4a = pd.concat([df_case11, df_case12])  # pd.merge(df_case11, df_case12, how='outer')
        df_4a = pd.concat([df_4a, df_case2])  # pd.merge(df, df_case2, how='outer')
        df_4a = pd.concat([df_4a, df_case3])  # pd.merge(df2, df_case3, how='outer')
        df_4a['latest_inventory_transaction'] = pd.to_datetime(latest_inventory_transaction, utc=True)
        if not df_4a.empty:
            df_4a.transaction_time = df_4a.apply(
                lambda x: x.first_put_time if pd.isna(x.transaction_time) else x.transaction_time, axis=1)
        df_4a.transaction_time = pd.to_datetime(df_4a.transaction_time, utc=True)
        df_4a.first_put_time = pd.to_datetime(df_4a.first_put_time, utc=True)
        df_4a = self.utilfunction.reset_index(df_4a)
        df_4a['untouched_ageing'] = None
        df_4a = self.calc_untouched_ageing2(df_4a)
        df_4a['slot_ageing'] = None
        df_4a = self.calc_slot_ageing(df_4a)
        df_4a['cron_ran_at'] = datetime.now()
        df_4a['slot_id'] = df_4a['slot']
        df_4a = df_4a.drop(columns=['index'])
        df_4a.rename(
            columns={'transaction_type': 'last_transaction_event', 'transaction_time': 'last_transaction_time'},
            inplace=True)
        # inner join
        df_joined = pd.merge(df, df_prev,  how='inner', on=['unique_id', 'station_type', 'storage_type'])
        df_joined = self.df_cleanup(df_joined)
        df_joined = df_joined.sort_values(by=['unique_id', 'time'], ascending=[True, False])
        df_joined = self.utilfunction.reset_index(df_joined)
        df_joined = df_joined.drop(columns=['index',
                                            'last_transaction_event', 'last_transaction_time',
                                            'remaining_uom', 'slot_ageing',
                                            'untouched_ageing', 'ID', 'transaction_id',
                                            'external_service_request_id', 'request_id',
                                            'event_name', 'user_name', 'delta_uom',
                                            'process_id', 'pps_id', 'ppsbin_id', 'start_date_time',
                                            'end_date_time'])

        m1 = df_joined['unique_id'].eq(df_joined['unique_id'].shift(-1))
        m2 = df_joined['transaction_type'].eq('put')
        m3 = df_joined['transaction_type'].shift(-1).eq('pick')
        m4 = df_joined['new_uom'].shift(-1).eq(0)
        m5 = df_joined['old_uom'].eq(0)
        df_joined['carry_flag'] = (m1 & m2 & m3 & m4 & m5).astype(int)
        m1 = df_joined['unique_id'].eq(df_joined['unique_id'].shift(-1))
        m2 = df_joined['transaction_type'].eq('pick')
        m3 = df_joined['transaction_type'].shift(-1).eq('put')
        m4 = df_joined['old_uom'].shift(-1).eq(0)
        m5 = df_joined['new_uom'].eq(0)
        df_joined['close_flag'] = (m1 & m2 & m3 & m4 & m5).astype(int)
        df_closeone = df_joined.loc[(df_joined['close_flag'] == 1)]
        df_closeone['latest_inventory_transaction'] = pd.to_datetime(current_transaction_time, utc=True)
        df_closeone['last_transaction_time'] = pd.to_datetime(df_closeone['time'], utc=True)
        df_closeone.rename(columns={'new_uom': 'remaining_uom',
                                    'transaction_type': 'last_transaction_event'}, inplace=True)
        ###check transaction time is blank
        df_closeone['untouched_ageing'] = None
        df_closeone = self.calc_untouched_ageing(df_closeone)
        df_closeone['slot_ageing'] = None
        df_closeone['first_put_time'] = pd.to_datetime(df_closeone['first_put_time'], utc=True)
        df_closeone = self.calc_slot_ageing(df_closeone)
        df_closezero = df_joined.loc[(df_joined['close_flag'] == 0)]
        df_carryone = df_closezero.loc[(df_closezero['carry_flag'] == 1)]
        df_carryzero = df_closezero.loc[(df_closezero['carry_flag'] == 0)]


        df_carryzero = df_carryzero.sort_values(by=['unique_id', 'time'], ascending=[True, False])
        df_carryzero['RecordID'] = np.arange(1, (df_carryzero.shape[0] + 1), 1)
        df_unique_pps_installation = df_carryzero[['unique_id', 'time', 'RecordID']]
        df_unique_pps_installation = df_unique_pps_installation.groupby(['unique_id'], as_index=False).agg(
            time=('time', 'first'), RecordID=('RecordID', 'first'))
        df_carryzero = pd.merge(df_carryzero, df_unique_pps_installation, how='inner',
                                on=['unique_id', 'time', 'RecordID'])
        del df_carryzero['RecordID']


        df_carryone = df_carryone.sort_values(by=['unique_id', 'time'], ascending=[True, False])
        df_carryone['RecordID'] = np.arange(1, (df_carryone.shape[0] + 1), 1)
        df_unique_pps_installation = df_carryone[['unique_id', 'time', 'RecordID']]
        df_unique_pps_installation = df_unique_pps_installation.groupby(['unique_id'], as_index=False).agg(
            time=('time', 'first'), RecordID=('RecordID', 'first'))
        df_carryone = pd.merge(df_carryone, df_unique_pps_installation, how='inner',
                               on=['unique_id', 'time', 'RecordID'])
        del df_carryone['RecordID']

        df_carryzero['remaining_uom'] = df_carryzero['new_uom']
        df_carryzero['transaction_time'] = df_carryzero['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df_carryone['left_unique_id'] = df_carryone['unique_id']
        df_left = pd.merge(df_carryzero, df_carryone[['unique_id', 'left_unique_id']], on='unique_id', how='left')
        if not df_left.empty:
            df_left = df_left.loc[(pd.isna(df_left['left_unique_id']) == True)]
        del df_left['left_unique_id']
        del df_carryone['left_unique_id']

        df_carryzero['right_unique_id'] = df_carryzero['unique_id']
        df_right = pd.merge(df_carryone, df_carryzero[['unique_id', 'right_unique_id']], on='unique_id', how='left')
        if not df_right.empty:
            df_right = df_right.loc[(pd.isna(df_right['right_unique_id']) == True)]
        del df_right['right_unique_id']
        del df_carryzero['right_unique_id']
        df_right['remaining_uom'] = df_right['first_put_uom']
        df_right['transaction_time'] = df_right['first_put_time']
        df_right['flag'] = 'untouched'
        df_left['flag'] = 'touched'
        df_carryone['right_time'] = df_carryone['time']
        df_carryone['right_transaction_type'] = df_carryone['transaction_type']
        df_carryone['right_new_uom'] = df_carryone['new_uom']
        df_carryone['right_rack_id'] = df_carryone['rack_id']
        df_carryone['right_rack_face'] = df_carryone['rack_face']
        df_carryone['right_slot'] = df_carryone['slot']
        df_carryone = df_carryone.drop(columns=['transaction_type', 'new_uom', 'rack_id', 'rack_face', 'slot'])
        df_inner = pd.merge(df_carryone[['unique_id', 'right_time', 'right_transaction_type', 'right_new_uom',
                                         'right_rack_id', 'right_rack_face', 'right_slot']], df_carryzero,
                            on='unique_id', how='inner')
        df_case12 = df_inner.loc[(df_inner['time'] > df_inner['right_time'])]  # alteryx true case
        df_case11 = df_inner.loc[(df_inner['time'] <= df_inner['right_time'])]
        # df_case12['first_put_uom'] = df_case12['remaining_uom']
        df_case12['first_put_time'] = df_case12['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df_case12['transaction_type'] = df_case12['right_transaction_type']
        df_case12['remaining_uom'] = df_case12['right_new_uom']
        df_case12['transaction_time'] = df_case12['right_time']
        df_case12 = df_case12.drop(
            columns=['right_slot', 'right_rack_face', 'right_transaction_type', 'right_new_uom', 'right_rack_id',
                     'right_time'])
        df_case12['flag'] = "touched"
        df_case12 = df_case12[(df_case12['remaining_uom'] > 0)]
        df_case11['first_put_time'] = df_case11['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df_case11['flag'] = "untouched"
        df_case11 = df_case11.drop(
            columns=['right_slot', 'right_rack_face', 'right_transaction_type', 'right_new_uom', 'right_rack_id',
                     'right_time'])
        df_case11.rename(
            columns={'transaction_type': 'last_transaction_event', 'transaction_time': 'last_transaction_time'},
            inplace=True)
        df_case12.rename(
            columns={'transaction_type': 'last_transaction_event', 'transaction_time': 'last_transaction_time'},
            inplace=True)
        df_right.rename(
            columns={'transaction_type': 'last_transaction_event', 'transaction_time': 'last_transaction_time'},
            inplace=True)
        df_left.rename(
            columns={'transaction_type': 'last_transaction_event', 'transaction_time': 'last_transaction_time'},
            inplace=True)
        df_case11 = df_case11.drop(columns=['carry_flag', 'close_flag', 'new_uom', 'old_uom', 'rack_face'])
        df_case12 = df_case12.drop(columns=['carry_flag', 'close_flag', 'new_uom', 'old_uom', 'rack_face'])
        df_right = df_right.drop(columns=['carry_flag', 'close_flag', 'new_uom', 'old_uom', 'rack_face'])
        df_left = df_left.drop(columns=['carry_flag', 'close_flag', 'new_uom', 'old_uom', 'rack_face'])
        df_closeone = df_closeone.drop(columns=['carry_flag', 'close_flag', 'old_uom', 'rack_face'])
        df_case11['latest_inventory_transaction'] = pd.to_datetime(current_transaction_time, utc=True)
        # df_left['latest_inventory_transaction'] = pd.to_datetime(current_transaction_time,utc=True)
        df_case11['untouched_ageing'] = None
        df_case11 = self.calc_untouched_ageing(df_case11)
        df_case11['slot_ageing'] = None
        df_case11 = self.calc_slot_ageing(df_case11)
        df_case12['untouched_ageing'] = None
        df_case12 = self.calc_untouched_ageing(df_case12)
        df_case12['slot_ageing'] = None
        df_case12 = self.calc_slot_ageing(df_case12)

        df_right['untouched_ageing'] = None
        df_right = self.calc_untouched_ageing(df_right)
        df_right['slot_ageing'] = None
        df_right = self.calc_slot_ageing(df_right)

        df_left['untouched_ageing'] = None
        df_left = self.calc_untouched_ageing(df_left)
        df_left['slot_ageing'] = None
        # df_left['first_put_time'] = df_left['first_put_time']
        df_left = self.calc_slot_ageing(df_left)
        df_merged = pd.concat([df_case11, df_case12])
        df_merged = pd.concat([df_merged, df_left])
        df_4c = pd.concat([df_merged, df_right])
        df_4a = pd.concat([df_4a, df_4b])
        final_df = pd.concat([df_4a, df_4c])
        df_4a = ''
        df_4b = ''
        df_4b = ''
        df_merged = ''
        df_left = ''
        df_case11 = ''
        df_case12 = ''
        if not df_closeone.empty:
            final_df = pd.concat([final_df, df_closeone])
        # final_df = final_df.reset_index()
        final_df = self.utilfunction.reset_index(final_df)
        if 'last_pick_event_time' not in final_df.columns:
            final_df['last_pick_event_time']=None

        if 'last_put_event_time' not in final_df.columns:
            final_df['last_put_event_time']=None

        if 'last_audit_event_time' not in final_df.columns:
            final_df['last_audit_event_time']=None

        #only for existing migration, later on its no required
        final_df['last_pick_event_time']=final_df.apply(lambda x:x['last_transaction_time'] if x['last_transaction_event']=='pick' else x['last_pick_event_time'], axis=1)
        final_df['last_put_event_time']=final_df.apply(lambda x:x['last_transaction_time'] if x['last_transaction_event']=='put' else x['last_put_event_time'], axis=1)
        final_df['last_audit_event_time']=final_df.apply(lambda x:x['last_transaction_time'] if x['last_transaction_event']=='audit' else x['last_audit_event_time'], axis=1)

        if 'index' in last_pick_df.columns:
            del last_pick_df['index']

        final_df= pd.merge(final_df,last_pick_df[last_pick_df['transaction_type']=='pick'][['unique_id','last_event_time_new']], on='unique_id',how='left')
        final_df['last_pick_event_time']=final_df.apply(lambda x:x['last_pick_event_time'] if pd.isna(x['last_event_time_new']) else x['last_event_time_new'] ,axis=1)
        del final_df['last_event_time_new']

        final_df= pd.merge(final_df,last_pick_df[last_pick_df['transaction_type']=='put'][['unique_id','last_event_time_new']], on='unique_id',how='left')
        final_df['last_put_event_time']=final_df.apply(lambda x:x['last_put_event_time'] if pd.isna(x['last_event_time_new']) else x['last_event_time_new'] ,axis=1)
        del final_df['last_event_time_new']

        final_df= pd.merge(final_df,last_pick_df[last_pick_df['transaction_type']=='audit'][['unique_id','last_event_time_new']], on='unique_id',how='left')
        final_df['last_audit_event_time']=final_df.apply(lambda x:x['last_audit_event_time'] if pd.isna(x['last_event_time_new']) else x['last_event_time_new'] ,axis=1)
        del final_df['last_event_time_new']

        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="Alteryx", timeout=600)

        q_cnt = f"select count(*)  from sku_details"
        temp_skus = pd.DataFrame(self.read_client.query(q_cnt).get_points())
        q_cnt = temp_skus['count_sku_id'][0]
        offset_val = 0
        data_limit = 10000
        while offset_val < q_cnt:
            q = f"select brand,sku_id,item_id from sku_details order by time desc limit {data_limit} offset {offset_val}"
            temp_skus = pd.DataFrame(self.read_client.query(q).get_points())
            temp_skus.drop_duplicates(subset=['item_id'], keep='first')
            if offset_val == 0:
                df_skus = temp_skus
            else:
                df_skus = pd.concat([df_skus, temp_skus])
            offset_val = offset_val + data_limit

        # q = f"select brand,sku_id,item_id from sku_details order by time desc"
        # df_skus = pd.DataFrame(self.read_client.query(q).get_points())
        df_skus = df_skus.drop_duplicates(subset=['item_id'], keep='first')
        final_df.item_id = final_df.item_id.astype(int)
        final_df = pd.merge(final_df, df_skus[['brand', 'sku_id', 'item_id']], how='left', on='item_id')
        final_df.sku_id = final_df.sku_id.apply(lambda x: 'N/A' if pd.isna(x) else x)
        final_df['season'] = final_df.sku_id
        final_df.season = final_df.season.astype(str)
        final_df = self.get_season(final_df)
        final_df['first_put_time'] = pd.to_datetime(final_df['first_put_time'])

        group_df = final_df.groupby(['item_id', 'slot_id'], as_index=False).agg(
            first_flag=('flag', 'first'), first_put_time=('first_put_time', 'first'),
            first_cron_ran_at=('cron_ran_at', 'first'))
        group_df['first_flag'] = group_df.first_flag.apply(lambda x: 1 if x == 'touched' else 0)
        group_df = group_df.groupby(['item_id'], as_index=False).agg(
            Concat_First_flag=('first_flag', 'max'), Min_First_first_put_time=('first_put_time', 'min'),
            first_cron_ran_at=('first_cron_ran_at', 'first'))
        group_df['sku_touched_untouched'] = group_df.Concat_First_flag.apply(
            lambda x: 'touched' if x == 1 else 'untouched')
        group_df['sku_ageing'] = None
        group_df = self.utilfunction.reset_index(group_df)
        group_df['first_cron_ran_at'] = pd.to_datetime(group_df['first_cron_ran_at'], utc=True)
        group_df['Min_First_first_put_time'] = pd.to_datetime(group_df['Min_First_first_put_time'])

        group_df = self.calc_sku_ageing(group_df)
        final_df = pd.merge(final_df, group_df[['sku_touched_untouched', 'sku_ageing', 'item_id']], how='left',
                            on='item_id')
        final_df = final_df.drop(columns=['index', 'slot'])
        cron_run_at = datetime.now()
        cron_run_at = cron_run_at.replace(microsecond=0)
        final_df['cron_ran_at'] = cron_run_at
        final_df = self.utilfunction.reset_index(final_df)
        final_df = self.utilfunction.get_epoch_time(final_df, date1=cron_run_at)
        # group_df.to_csv(r'/home/ankush.j/Desktop/workname/big_data/group_df.csv', index=False, header=True)

        final_df.time = pd.to_datetime(final_df.time_ns)
        final_df = final_df.drop(columns=['time_ns', 'index'])
        # final_df=final_df[['time','rack_id','item_id']]
        # rackinfo=self.fetch_rack_info_from_tower()
        final_df = pd.merge(final_df, rackinfo, on=["rack_id"])
        final_df['racktype'] = final_df['rack_type']
        del final_df['rack_type']
        final_df.time = pd.to_datetime(final_df.time)
        final_df['untouched_ageing'] = final_df['untouched_ageing'].fillna(0)
        final_df['slot_ageing'] = final_df['slot_ageing'].fillna(0)
        final_df['sku_ageing'] = final_df['sku_ageing'].fillna(0)
        final_df['untouched_ageing'] = final_df['untouched_ageing'].astype(float)
        final_df['slot_ageing'] = final_df['slot_ageing'].astype(float)
        final_df['sku_ageing'] = final_df['sku_ageing'].astype(float)
        final_df['first_put_uom'] = final_df['first_put_uom'].astype(float)
        final_df['remaining_uom'] = final_df['remaining_uom'].astype(float)
        final_df['item_id'] = final_df['item_id'].astype(str)
        final_df['slot_id'] = final_df['slot_id'].astype(str)
        final_df['tpid'] = final_df['tpid'].astype(str)
        # final_df['racktype'] = final_df['racktype'].fillna('')
        final_df.latest_inventory_transaction = final_df.latest_inventory_transaction.apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%SZ'))
        final_df.first_put_time = final_df.first_put_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        final_df.last_transaction_time = pd.to_datetime(final_df.last_transaction_time)
        final_df.last_transaction_time = final_df.last_transaction_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

        final_df.last_pick_event_time = pd.to_datetime(final_df.last_pick_event_time, utc=True)
        final_df.last_pick_event_time = final_df.last_pick_event_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        final_df.last_put_event_time = pd.to_datetime(final_df.last_put_event_time, utc=True)
        final_df.last_put_event_time = final_df.last_put_event_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        final_df.last_audit_event_time = pd.to_datetime(final_df.last_audit_event_time, utc=True)
        final_df.last_audit_event_time = final_df.last_audit_event_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        final_df.cron_ran_at = final_df.cron_ran_at.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        #         # final_df['record_id'] = np.arange(1, (final_df.shape[0] + 1), 1)
        #         final_df['last_transaction_time'] = current_transaction_time

        if not final_df.empty:
            l = ['level_0','index','index_x','index_y']
            for col in l:
                if col in final_df.columns:
                    del final_df[col]

        final_df = final_df.set_index('time')

        if not final_df.empty:
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])

            # try:
            #     self.client.query("drop measurement slots_ageing_ayx_latest")
            # except:
            #     pass
            touched_item_count = final_df[final_df['sku_touched_untouched'] == 'touched']['item_id'].nunique()
            untouched_item_count = final_df[final_df['sku_touched_untouched'] == 'untouched']['item_id'].nunique()
            total_remaining_uom = final_df['remaining_uom'].sum()
            touched_unique_ids = \
            final_df[(final_df['flag'] == 'touched') & (final_df['remaining_uom'] > 0)][
                'unique_id'].nunique()
            untouched_unique_ids = \
                final_df[(final_df['flag'] == 'untouched') & (final_df['remaining_uom'] > 0)][
                    'unique_id'].nunique()

            summary_df = pd.DataFrame([{
                'installation_id': 'butler_demo',
                'time': cron_run_at,
                'cron_ran_at': cron_run_at,
                'touched_item_count': int(touched_item_count),
                'untouched_item_count': int(untouched_item_count),
                'total_remaining_uom': int(total_remaining_uom),
                'touched_unique_ids': int(touched_unique_ids),
                'untouched_unique_ids': int(untouched_unique_ids)
            }])
            summary_df['cron_ran_at'] = summary_df['cron_ran_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
            summary_df.time = pd.to_datetime(summary_df.time)
            summary_df = summary_df.set_index('time')

            self.write_client.writepoints(summary_df, "inventory_summary", db_name=self.tenant_info["out_db_name"],
                                          tag_columns=[], dag_name=os.path.basename(__file__),
                                          site_name=self.tenant_info['Name'], retention_policy=rp_seven_days)

            self.write_client.writepoints(final_df, "slots_ageing_ayx_latest", db_name=self.tenant_info["out_db_name"],
                                          tag_columns=['rack_id'], dag_name=os.path.basename(__file__),
                                          site_name=self.tenant_info['Name'],retention_policy =rp_seven_days)
            temp_date = datetime.now()
            #store data once in month
            if temp_date.day == 2 and temp_date.hour >14:
                final_df['index_x']=0
                final_df['index_y']=1.0

                self.write_client.writepoints(final_df, "slots_ageing_ayx", db_name=self.tenant_info["out_db_name"],
                                              tag_columns=['rack_id'], dag_name=os.path.basename(__file__),
                                              site_name=self.tenant_info['Name'])
        sl_util = slot_utilzation()
        sl_util.slot_utilzation_calc(tenant_info, self.end_date)
        return None


with DAG(
        'slot_againg',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of puts per rack face GM-44025',
        schedule_interval=timedelta(hours=8),
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
            if tenant['Active'] == "Y" and tenant['slot_againg'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                final_task = PythonOperator(
                    task_id='slot_againg_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(slot_againg().slot_againg_final,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=9600),
                )
    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='slot_againg_final',
            provide_context=True,
            python_callable=functools.partial(slot_againg().slot_againg_final, tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )