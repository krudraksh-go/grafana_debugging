## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

from utils.CommonFunction import InfluxData,Write_InfluxData, postgres_connection, CommonFunction
import numpy as np
from slot_utilization_ayx import slot_utilzation
import pytz
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
from config import (CONFFILES_DIR)
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name =os.environ.get('Out_db_name', 'airflow')
Postgres_ButlerDev=os.environ.get('Postgres_ButlerDev', Butler_ip)
Postgres_butler_port=os.environ.get('Postgres_butler_port', 5432)
Postgres_butler_password=os.environ.get('Postgres_butler_password', "YLafmbfuji$@#45!")
Postgres_butler_user=os.environ.get('Postgres_butler_user', "altryx_read")
Tower_password=os.environ.get('Tower_password', "YLafmbfuji$@#45!")
Tower_user=os.environ.get('Tower_password', "altryx_read")
Postgres_tower_port=os.environ.get('Postgres_tower_port', 5432)
Postgres_tower=os.environ.get('Postgres_tower', "localhost")
streaming=os.environ.get('streaming', "N")
TimeZone=os.environ.get('TimeZone', 'PDT')
sslrootcert=os.environ.get('sslrootcert', "")
sslcert=os.environ.get('sslcert', "")
sslkey=os.environ.get('sslkey', "")
from config import (
    rp_seven_days
)
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
class reserve_slot_ageing_one_time:
    def divide_slotref(self, df):
        df['rack_id'] = df['slot'].str.split('.', expand=True)[0]
        df['rack_face'] = df['slot'].str.split('.', expand=True)[1]
#        df['slot'] = df['slot_id'].str.split('.', 2, expand=True)[2]
        return df

    def get_season(self, df):
        for i in df.index:
            season=df['season'][i]
            if len(season)>=3:
                season= season.replace(season[-3:],'')
                season=season[-3:]
                df['season'][i]=season
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
            df["latest_inventory_transaction"]=pd.to_datetime(df["latest_inventory_transaction"], utc=True)
            df["last_transaction_time"] = pd.to_datetime(df["last_transaction_time"], utc=True)
            df = self.utilfunction.datediff_in_sec(df, "latest_inventory_transaction", "last_transaction_time","untouched_ageing")
            df['untouched_ageing'] = df['untouched_ageing'] / 3600
        return df

    def calc_untouched_ageing2(self, df):
        if not df.empty:
            df["latest_inventory_transaction"]=pd.to_datetime(df["latest_inventory_transaction"], utc=True)
            df["transaction_time"] = pd.to_datetime(df["transaction_time"], utc=True)
            df = self.utilfunction.datediff_in_sec(df, "latest_inventory_transaction", "transaction_time","untouched_ageing")
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

    def reserve_slot_ageing_one_time_final(self, tenant_info, **kwargs ):
        self.tenant_info = tenant_info['tenant_info']
        self.utilfunction = CommonFunction()
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        self.end_date = datetime.now(timezone.utc)
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        columnlist=['ID', 'transaction_id', 'external_service_request_id', 'transaction_type',
                                           'request_id', 'event_name', 'user_name', 'slot', 'tpid', 'item_id',
                                           'old_uom', 'delta_uom', 'new_uom', 'process_id', 'time', 'pps_id',
                                           'ppsbin_id','unique_id','start_date_time','end_date_time','rack_id','rack_face']
        columnlist2=['ID', 'transaction_id', 'external_service_request_id', 'transaction_type',
                                           'request_id', 'event_name', 'user_name', 'slot', 'tpid', 'item_id',
                                           'old_uom', 'delta_uom', 'new_uom', 'process_id', 'time', 'pps_id',
                                           'ppsbin_id']

        if self.tenant_info['Postgres_butler_user'] != "":
            qry = f"select count(*) from inventory_transaction_archives"

            self.postgres_conn = postgres_connection(database='butler_dev',
                                                     user='altryx_read', \
                                                     host='192.168.5.57', \
                                                     port='5433',\
                                                     password='YLafmbfuji$@#45!', \
                                                     sslrootcert=None,\
                                                     sslcert=None, \
                                                     sslkey=None,
                                                     dag_name=os.path.basename(__file__),
                                                     site_name=self.tenant_info['Name'])

            totalcount = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
            self.postgres_conn.close()
            f = True
            time_offset = 0
            l = 10000
            print(f"total orders:{totalcount[0][0]}")
            tempcount = totalcount[0][0]
            while f == True:
                if time_offset <= tempcount:
                    qry = f"select id,transaction_id,external_service_request_id,transaction_type,\
                    request_id,event_name, user_name, slot, tpid, item_id, old_uom, delta_uom, new_uom, \
                    process_id, time, pps_id , ppsbin_id from inventory_transaction_archives \
                    order by time  offset "
                    qry = qry + str(time_offset) + " limit " + str(l)
                    print(qry)
                    self.postgres_conn = postgres_connection(database='butler_dev',
                                                             user='altryx_read', \
                                                             host='192.168.5.57', \
                                                             port='5433', \
                                                             password='YLafmbfuji$@#45!', \
                                                             sslrootcert=None,\
                                                             sslcert=None, \
                                                             sslkey=None,
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
            print(df)
            df = self.utilfunction.reset_index(df)
            current_transaction_time = df['time'].iloc[-1]

            df =self.divide_slotref(df)
            df['old_uom'] = df.old_uom.astype(str).str.extract('(\d+)}$', expand=False)
            df['new_uom'] = df.new_uom.astype(str).str.extract('(\d+)}$', expand=False)
            df['delta_uom'] = df.delta_uom.astype(str).str.extract('(\d+)}$', expand=False)
            df['item_id']=df['item_id'].astype(str)
            df['unique_id'] = df['item_id'] + '_' + df['slot']
            df['start_date_time'] = self.end_date
            df['end_date_time'] = self.end_date
        else:
            df = pd.DataFrame(columns=columnlist)


        df['old_uom']=df['old_uom'].fillna(0).astype(int)
        df['new_uom'] = df['new_uom'].fillna(0).astype(int)
        df['delta_uom'] = df['delta_uom'].fillna(0).astype(int)
        current_transaction_time=pd.to_datetime(current_transaction_time)
        latest_inventory_transaction = pd.to_datetime(current_transaction_time)

        #left not matched data(alteryx right join)--first record
        df['latest_inventory_transaction']=pd.to_datetime(latest_inventory_transaction, utc=True)
        df['time'] = pd.to_datetime(df['time'],utc=True)
        # ## now left join for 2nd record
        # path = f'{self.tenant_info["Name"]}_files.csv'
        # path = os.path.join(CONFFILES_DIR, path)
        # df.to_csv(path, index=True, header=True)
        dfb=df
        dfb['old_uom']=dfb['old_uom'].astype(int)
        dfb['new_uom'] = dfb['new_uom'].astype(int)
        df_case1 = dfb.loc[(dfb['transaction_type'] == 'put') & (dfb['old_uom'] == 0)]
        df_case1['RecordID'] = np.arange(1, (df_case1.shape[0] + 1), 1)
        df_case1 = df_case1.sort_values(by=['unique_id', 'time'], ascending=[True, False])
        df_unique_pps_installation = df_case1[['unique_id', 'time', 'RecordID']]
        df_unique_pps_installation = df_unique_pps_installation.groupby(['unique_id'], as_index=False).agg(
            time=('time', 'first'), RecordID=('RecordID', 'first'))
        df_case1 = pd.merge(df_case1, df_unique_pps_installation, how='inner',
                                on=['unique_id', 'time', 'RecordID'])
        del df_case1['RecordID']
        dfb = dfb.loc[(dfb['transaction_type'] != 'put') | (dfb['old_uom'] != 0)]
        dfb = dfb.sort_values(by=['unique_id', 'time'], ascending=[True, False])
        dfb['RecordID'] = np.arange(1, (dfb.shape[0] + 1), 1)
        df_unique_pps_installation = dfb[['unique_id','time','RecordID']]
        df_unique_pps_installation = df_unique_pps_installation.groupby(['unique_id'], as_index=False).agg(time=('time', 'first'), RecordID=('RecordID', 'first'))
        dfb=pd.merge(dfb,df_unique_pps_installation , how='inner', on=['unique_id','time','RecordID'])
        del dfb['RecordID']
        dfb['right_time'] = dfb['time']
        dfb['right_transaction_type'] = dfb['transaction_type']
        dfb['right_new_uom'] = dfb['new_uom']
        dfb['right_rack_id'] = dfb['rack_id']
        dfb['right_rack_face'] = dfb['rack_face']
        dfb['right_slot'] = dfb['slot']
        df_inner = pd.merge(df_case1, dfb[['unique_id','right_time','right_transaction_type','right_new_uom']], how='inner', on='unique_id')
        df_case3 = pd.merge(df_case1, dfb[['unique_id','right_time']], how='left', on='unique_id')
        df_case3 = df_case3.loc[(pd.isna(df_case3['right_time']) == True)]
        del df_case3['right_time']
        df_case1['left_time'] = df_case1['time']
        df_case2 = pd.merge(df_case1[['unique_id','left_time']], dfb, how='right', on='unique_id')
        df_case2 = df_case2.loc[(pd.isna(df_case2['left_time']) == True)]
        del df_case2['left_time']
        df_case11 = df_inner.loc[(df_inner['time'] >= df_inner['right_time'])]
        df_case12 = df_inner.loc[(df_inner['time'] < df_inner['right_time'])]
        df_case12 = df_case12.drop(
            columns=['ID', 'transaction_id', 'external_service_request_id', 'transaction_type', 'request_id',
                     'event_name', 'user_name', 'old_uom', 'delta_uom', 'process_id', 'pps_id', 'ppsbin_id',
                     'rack_face', 'start_date_time', 'end_date_time'])
        df_case12['first_put_uom'] = df_case12['new_uom']
        df_case12['first_put_time'] = df_case12['time'].apply(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
        df_case12['transaction_type'] = df_case12['right_transaction_type']
        df_case12['remaining_uom'] = df_case12['right_new_uom']
        df_case12['transaction_time'] = df_case12['right_time']
        df_case12=df_case12.drop(columns=['new_uom', 'right_transaction_type','right_new_uom','right_time'])
        df_case12['flag'] = "Touched"


        df_case11 = df_case11.drop(columns=['ID', 'transaction_id', 'external_service_request_id',
                                        'request_id', 'event_name', 'user_name',
                                        'old_uom', 'delta_uom', 'process_id',
                                        'pps_id', 'ppsbin_id', 'rack_face',
                                        'start_date_time', 'end_date_time', 'right_time', 'right_transaction_type',
                                        'right_new_uom'
                                        ])
        df_case11['first_put_uom'] = df_case11['new_uom']
        df_case11['first_put_time'] = df_case11['time'].apply(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
        df_case11['remaining_uom'] = df_case11['first_put_uom']
        df_case11['transaction_time'] = df_case11['time'].apply(lambda x:x.strftime('%Y-%m-%d %H:%M:%S')) ##need to check
        df_case11['flag'] = "untouched"
        df_case11 = df_case11[(df_case11['remaining_uom'] > 0)]

        df_case11 = df_case11.drop(columns=['new_uom'])
        df_case3 = df_case3.drop(columns=['ID', 'transaction_id', 'external_service_request_id',
                                            'request_id', 'event_name', 'user_name',
                                            'old_uom', 'delta_uom', 'process_id',
                                            'pps_id', 'ppsbin_id', 'rack_face',
                                            'start_date_time', 'end_date_time'
                                            ])
        df_case3['first_put_time'] = df_case3['time'].apply(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
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
                                            'start_date_time', 'end_date_time', 'right_time', 'right_transaction_type',
                                            'right_new_uom', 'right_rack_id', 'right_rack_face', 'right_slot',
                                            ])

        df_case2['first_put_uom'] = df_case2['new_uom']
        df_case2['remaining_uom'] = df_case2['first_put_uom']
        df_case2['first_put_time'] = df_case2['time'].apply(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
        df_case2['remaining_uom']=df_case2['remaining_uom'].fillna(0)
        df_case2['remaining_uom']=df_case2['remaining_uom'].astype('int')
        df_case2 = df_case2[(df_case2['remaining_uom'] > 0)]
        df_case2 = df_case2.drop(columns=['new_uom'])
        df_4a = pd.concat([df_case11,df_case12]) #pd.merge(df_case11, df_case12, how='outer')
        df_4a = pd.concat([df_4a,df_case2]) #pd.merge(df, df_case2, how='outer')
        df_4a =pd.concat([df_4a,df_case3]) #pd.merge(df2, df_case3, how='outer')
        df_4a['latest_inventory_transaction'] = pd.to_datetime(latest_inventory_transaction,utc=True)
        if not df_4a.empty:
            df_4a.transaction_time = df_4a.apply(lambda x: x.first_put_time if pd.isna(x.transaction_time) else x.transaction_time, axis=1 )
        df_4a.transaction_time =pd.to_datetime(df_4a.transaction_time, utc=True)
        df_4a.first_put_time = pd.to_datetime(df_4a.first_put_time, utc=True)
        df_4a= df_4a.reset_index()
        df_4a['untouched_ageing'] = None
        df_4a = self.calc_untouched_ageing2(df_4a)
        df_4a['slot_ageing'] = None
        df_4a = self.calc_slot_ageing(df_4a)
        df_4a['cron_ran_at'] = datetime.now()
        df_4a['slot_id'] = df_4a['slot']
        df_4a = df_4a.drop(columns=['index'])
        df_4a.rename(columns={'transaction_type': 'last_transaction_event', 'transaction_time': 'last_transaction_time'}, inplace=True)
        final_df = df_4a.reset_index()
        print("final_df")
#        print(final_df)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="Alteryx")


        q = f"select brand,sku_id,item_id from sku_details order by time desc"
        df_skus = pd.DataFrame(self.read_client.query(q).get_points())
        df_skus=df_skus.drop_duplicates(subset=['item_id'],  keep='first')
        final_df.item_id= final_df.item_id.astype(int, errors='ignore')
        df_skus.item_id= df_skus.item_id.astype(int, errors='ignore')
        final_df = pd.merge(final_df,df_skus[['brand','sku_id','item_id']], how='left', on='item_id')
        final_df.sku_id = final_df.sku_id.apply(lambda x: 'N/A' if pd.isna(x) else x)
        final_df['season']= final_df.sku_id
        final_df.season = final_df.season.astype(str)
        final_df = self.get_season(final_df)
        final_df['first_put_time']=pd.to_datetime(final_df['first_put_time'])

        group_df = final_df.groupby(['item_id', 'slot_id'], as_index=False).agg(
            first_flag=('flag', 'first'),first_put_time=('first_put_time', 'first'),first_cron_ran_at=('cron_ran_at', 'first'))
        group_df['first_flag']=group_df.first_flag.apply(lambda x:1 if x=='touched' else 0)
        group_df = group_df.groupby(['item_id'], as_index=False).agg(
            Concat_First_flag=('first_flag', 'max'),Min_First_first_put_time=('first_put_time', 'min'),first_cron_ran_at=('first_cron_ran_at', 'first'))
        group_df['sku_touched_untouched'] = group_df.Concat_First_flag.apply(lambda x: 'touched' if x ==1  else 'untouched')
        group_df['sku_ageing']=None
        group_df=group_df.reset_index()
        group_df['first_cron_ran_at']= pd.to_datetime(group_df['first_cron_ran_at'], utc=True)
        group_df['Min_First_first_put_time'] = pd.to_datetime(group_df['Min_First_first_put_time'])

        group_df= self.calc_sku_ageing(group_df)
        final_df = pd.merge(final_df, group_df[['sku_touched_untouched', 'sku_ageing', 'item_id']], how='left', on='item_id')
        final_df=final_df.drop(columns=['index','slot'])
        cron_run_at= datetime.now()
        cron_run_at = cron_run_at.replace(microsecond=0)
        final_df['cron_ran_at'] = cron_run_at
        final_df=self.utilfunction.reset_index(final_df)
        final_df=self.utilfunction.get_epoch_time(final_df,date1=cron_run_at)
        # final_df.to_csv(r'/home/ankush.j/Desktop/workname/big_data/final_df.csv', index=False, header=True)
        # group_df.to_csv(r'/home/ankush.j/Desktop/workname/big_data/group_df.csv', index=False, header=True)

        final_df.time = pd.to_datetime(final_df.time_ns)
        final_df=final_df.drop(columns=['time_ns','index'])
        #final_df=final_df[['time','rack_id','item_id']]
#        rackinfo=self.fetch_rack_info_from_tower()
#        final_df=pd.merge(final_df,rackinfo,on=["rack_id"], how='left')
#        final_df['racktype'] =final_df['rack_type']
#        del final_df['rack_type']
        final_df.time = pd.to_datetime(final_df.time)
        final_df['untouched_ageing']=final_df['untouched_ageing'].fillna(0)
        final_df['slot_ageing'] = final_df['slot_ageing'].fillna(0)
        final_df['sku_ageing'] = final_df['sku_ageing'].fillna(0)
        final_df['untouched_ageing']=final_df['untouched_ageing'].astype(float)
        final_df['slot_ageing'] = final_df['slot_ageing'].astype(float)
        final_df['sku_ageing'] = final_df['sku_ageing'].astype(float)
        final_df['first_put_uom'] = final_df['first_put_uom'].astype(float)
        final_df['remaining_uom'] = final_df['remaining_uom'].astype(float)
        final_df['item_id'] = final_df['item_id'].astype(str)
        final_df['slot_id'] = final_df['slot_id'].astype(str)
        final_df['tpid'] = final_df['tpid'].astype(str)
        final_df['sku_id'] = final_df['sku_id'].astype(str)
        final_df.latest_inventory_transaction = final_df.latest_inventory_transaction.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%SZ'))
        final_df.first_put_time = final_df.first_put_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        final_df.last_transaction_time = pd.to_datetime(final_df.last_transaction_time)
        final_df.last_transaction_time = final_df.last_transaction_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        final_df.cron_ran_at = final_df.cron_ran_at.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        if 'level_0' in final_df:
            del final_df['level_0']
        final_df = final_df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
        if not final_df.empty:
            # try:
            #     self.client.query("drop measurement reserve_slots_ageing_ayx_latest")
            # except:
            #     pass
            self.write_client.writepoints(final_df, "reserve_slots_ageing_ayx_latest", db_name=self.tenant_info["out_db_name"],tag_columns=['rack_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'],retention_policy =rp_seven_days)
        return None


with DAG(
    'reserve_slot_ageing_one_time',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'creserve_slot_ageing_one_time',
    schedule_interval = None,
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
            if tenant['Active'] == "Y" and tenant['Name'] == "HnMCanada":
                final_task = PythonOperator(
                    task_id='reserve_slot_ageing_one_time_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(reserve_slot_ageing_one_time().reserve_slot_ageing_one_time_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=7200),
                )
    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='reserve_slot_ageing_one_time_final',
            provide_context=True,
            python_callable=functools.partial(reserve_slot_ageing_one_time().reserve_slot_ageing_one_time_final,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )


#
