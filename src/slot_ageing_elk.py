## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, postgres_connection, CommonFunction, Elasticsearch
import numpy as np
from slot_storage_utilization_elk import slot_utilzation
import pytz
from codecs import ignore_errors
from warehouse_inventory import warehouse_inventory

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
#     'slot_ageing',
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
class slot_ageing:
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

    def slot_ageing_final(self, tenant_info, **kwargs):
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

        if self.client.is_table_present('rp_seven_days.slots_ageing_elk'):
            table_name = 'rp_seven_days.slots_ageing_elk'
        elif self.client.is_table_present('slots_ageing_elk'):
            table_name = 'slots_ageing_elk'
        else:
            table_name = 'rp_seven_days.slots_ageing_ayx_latest'

        q = f"select * from {table_name} where time > now()-1d and host = '{self.tenant_info['host_name']}' order by time desc limit 1"
        cron_run_at = pd.DataFrame(self.client.query(q).get_points())
        if cron_run_at.empty:
            q = f"select * from {table_name} where time > now()-7d order by time desc limit 1"
            cron_run_at = pd.DataFrame(self.client.query(q).get_points())

        self.end_date = datetime.now(timezone.utc)
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        if not cron_run_at.empty:
            last_transaction_time = cron_run_at["last_transaction_time"][0]
            latest_inventory_transaction = pd.to_datetime(cron_run_at["latest_inventory_transaction"][0])
            self.start_date = cron_run_at["cron_ran_at"][0]
            current_transaction_time = cron_run_at['time'][0]
            # self.start_date = self.client.get_start_date("slots_utilization_ayx")
            # self.end_date = datetime.now(timezone.utc)
            # self.end_date = self.end_date.replace(minute=0,second=0)
            # self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
            #
            # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')


            if "slots_ageing_elk" in table_name:
                q_cnt = f"select count(cron_ran_at) from {table_name} where time >= '{self.start_date}' and time<= '{self.end_date}' and remaining_uom!='0' and host = '{self.tenant_info['host_name']}' order by time desc"
            else:
                q_cnt = f"select count(cron_ran_at) from {table_name} where time >= '{self.start_date}' and time<= '{self.end_date}' and remaining_uom>0 order by time desc"

            temp_slotdata = pd.DataFrame(self.client.query(q_cnt).get_points())
            q_cnt = temp_slotdata['count'][0]
            offset_val = 0
            data_limit = 5000
            temp_date = self.start_date

            while offset_val < q_cnt:
                # Construct base query
                if "slots_ageing_elk" in table_name:
                    if offset_val==0:
                        query = (
                            f"SELECT unique_id, first_put_time, first_put_uom "
                            f"FROM {table_name} "
                            f"WHERE time >= '{temp_date}' AND time <= '{self.end_date}' "
                            f"AND remaining_uom != '0' "
                            f"AND host = '{self.tenant_info['host_name']}'"
                            f" ORDER BY time LIMIT {data_limit}"
                        )
                    else:
                        query = (
                            f"SELECT unique_id, first_put_time, first_put_uom "
                            f"FROM {table_name} "
                            f"WHERE time > '{temp_date}' AND time <= '{self.end_date}' "
                            f"AND remaining_uom != '0' "
                            f"AND host = '{self.tenant_info['host_name']}'"
                            f" ORDER BY time LIMIT {data_limit}"
                        )

                else:
                    if offset_val == 0:
                        query = (
                            f"SELECT unique_id, first_put_time, first_put_uom "
                            f"FROM {table_name} "
                            f"WHERE time >= '{temp_date}' AND time <= '{self.end_date}' "
                            f"AND remaining_uom > 0 "
                            f"ORDER BY time LIMIT {data_limit}"
                        )
                    else:
                        query = (
                            f"SELECT unique_id, first_put_time, first_put_uom "
                            f"FROM {table_name} "
                            f"WHERE time > '{temp_date}' AND time <= '{self.end_date}' "
                            f"AND remaining_uom > 0 "
                            f"ORDER BY time LIMIT {data_limit}"
                        )

                # Execute query and fetch results
                temp_slotdata = pd.DataFrame(self.client.query(query).get_points())

                if temp_slotdata.empty:
                    break

                # Update the temp_date to the latest timestamp
                temp_date = temp_slotdata['time'].iloc[-1]

                # Concatenate the result
                if offset_val == 0:
                    df_prev = temp_slotdata
                else:
                    df_prev = pd.concat([df_prev, temp_slotdata])

                # Update offset
                offset_val += data_limit
        else:
            df_prev=  pd.DataFrame(columns=['time','unique_id','first_put_time','first_put_uom'])

        rackinfo = self.fetch_rack_info_from_tower()
        elastic_client = Elasticsearch(host=self.tenant_info["elastic_ip"], port=self.tenant_info["elastic_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info["Name"])
        all_hits = elastic_client.fetch_all_docs('inventory-v2', batch_size=5000)
        print(f"Total hits: {len(all_hits)}")
        #modified_hits = self.add_additional_fields(all_hits)
        if len(all_hits)>0:
            df2 = pd.DataFrame(all_hits)
            df2['item_id'] = df2['_source'].apply(lambda x: x['product_uid'])
            df2['last_sku_transaction_event'] = df2['_source'].apply(lambda x: x['last_activity'])
            df2['last_sku_transaction_time'] = df2['_source'].apply(lambda x: x['last_activity'])
            df2['sku_id'] = df2['_source'].apply(lambda x: x['pdfa']['product_sku'])
            df2['tpid'] = df2['_source'].apply(lambda x: x['last_event_data']['tpid'])
            df2['storage'] = df2['_source'].apply(lambda x: x['storage'])
            df2 = df2.drop(columns=['_index', '_type', '_id', '_score', '_source'])
            df_exploded = df2.explode('storage')
            df_exploded = df_exploded.reset_index()
            df_exploded['station_type'] = df_exploded['storage'].apply(
                lambda x: x['fulfilmentZone'] if type(x) == dict and 'fulfilmentZone' in x.keys() else '')
            df_exploded['rack_id'] = df_exploded['storage'].apply(lambda x: x['rack_id'] if type(x) == dict else x)
            df_exploded['slot_id'] = df_exploded['storage'].apply(lambda x: x['slot_id'] if type(x) == dict else x)
            df_exploded['storage_type'] = df_exploded['storage'].apply(
                lambda x: x['carrierType'] if type(x) == dict and 'carrierType' in x.keys() else '')
            df_exploded['remaining_uom'] = df_exploded['storage'].apply(
                lambda x: x['uom_quantity'] if type(x) == dict and 'uom_quantity' in x else x)
            df_exploded['last_transaction_event'] = df_exploded['storage'].apply(
                lambda x: x['last_activity'] if type(x) == dict else x)
            df_exploded['last_transaction_time'] = df_exploded['storage'].apply(
                lambda x: x['last_activity'] if type(x) == dict else x)
            df_exploded['last_sku_transaction_event'] = df_exploded['last_sku_transaction_event'].apply(
                lambda x: list(x.keys())[0])
            df_exploded['last_transaction_event'] = df_exploded['last_transaction_event'].apply(
                lambda x: list(x.keys())[0] if not pd.isna(x) and not bool(x) and len(list(x.keys()))>0  else x)
            df_exploded = df_exploded[~pd.isna(df_exploded['last_transaction_event'])]
            df_exploded['last_sku_transaction_time'] = df_exploded['last_sku_transaction_time'].apply(
                lambda x: x[list(x.keys())[0]])
            df_exploded['last_transaction_time'] = df_exploded['last_transaction_time'].apply(
                lambda x: x[list(x.keys())[0]] if not pd.isna(x) and len(list(x.keys()))>0 else x)
            df_exploded['tags'] = df_exploded['storage'].apply(
                lambda x: x['tags'] if type(x) == dict and 'tags' in x.keys() else '')
            df_exploded['productionOrderNumber'] = df_exploded['tags'].apply(
                lambda x: x['productionOrderNumber'] if type(x) == dict and 'productionOrderNumber' in x.keys() else '')
            del df_exploded['tags']
            df_exploded['remaining_uom'] = df_exploded['remaining_uom'].apply(lambda x: x if not pd.isna(x) and bool(x) else '0')
            df_exploded['remaining_item_int'] = df_exploded['remaining_uom'].apply(lambda x: x['Item'] if x!='0' and 'Item' in list(x.keys())  else 0)
            df_exploded['item_id'] = df_exploded['item_id'].astype(str)
            df_exploded['unique_id'] = df_exploded['item_id'] + '_' + df_exploded['slot_id']
            df_exploded = df_exploded.drop(columns=['storage'])
            df_prev = df_prev.groupby(['unique_id'], as_index=False).agg(first_put_time=('first_put_time', 'first'),
                                                               first_put_uom=('first_put_uom', 'first'))
            df_exploded = pd.merge(df_exploded, df_prev, how='left', on=['unique_id'])
            df_exploded['slot_touched_untouched'] = df_exploded.apply(
                lambda x: 'untouched' if pd.isna(x['first_put_time']) or x['first_put_time'] == '' else 'touched', axis=1)
            df_exploded['flag'] = df_exploded['slot_touched_untouched'].apply(lambda x: 1 if x=='touched' else 0)
            df_exploded['first_put_time'] = df_exploded.apply(
                lambda x: x['last_transaction_time'] if pd.isna(x['first_put_time']) or x['first_put_time'] == '' else x[
                    'first_put_time'], axis=1)
            df_exploded['first_put_uom'] = df_exploded.apply(
                lambda x: x['remaining_uom'] if pd.isna(x['first_put_uom']) or x['first_put_time'] == '' else x[
                    'first_put_uom'], axis=1)
            df_exploded['first_put_time'] = df_exploded['first_put_time'].apply(lambda x: '' if not x else x)
            if 'first_put_time' in df_exploded.columns:
                df_exploded = df_exploded[df_exploded['first_put_time'] != '']
            df_exploded_min_first_put = df_exploded.groupby(['item_id'], as_index=False).agg(
                Min_First_first_put_time=('first_put_time', 'min'),sku_touched_untouched=('flag','max'))
            df_exploded = pd.merge(df_exploded, df_exploded_min_first_put, how='left', on=['item_id'])
            df_exploded['sku_touched_untouched']=df_exploded['sku_touched_untouched'].apply(lambda x:'touched' if x==1 else 'untouched')
            if 'flag' in df_exploded.columns:
                del df_exploded['flag']
            df_exploded['latest_inventory_transaction'] = datetime.now(timezone.utc)
            df_exploded['Min_First_first_put_time'] = pd.to_datetime(df_exploded['Min_First_first_put_time'], utc=True, errors='coerce')
            df_exploded['latest_inventory_transaction'] = pd.to_datetime(df_exploded['latest_inventory_transaction'],
                                                                         utc=True)
            df_exploded['last_transaction_time'] = pd.to_datetime(df_exploded['last_transaction_time'], utc=True, errors='coerce')
            df_exploded['first_put_time'] = pd.to_datetime(df_exploded['first_put_time'], utc=True, errors='coerce')

            df_exploded['sku_ageing'] = None
            df_exploded['slot_ageing'] = None
            df_exploded['untouched_ageing'] = None
            df_exploded = self.utilfunction.datediff_in_sec(df_exploded, "latest_inventory_transaction",
                                                            "Min_First_first_put_time", "sku_ageing")
            df_exploded = self.utilfunction.datediff_in_sec(df_exploded, "latest_inventory_transaction", "first_put_time",
                                                            "slot_ageing")
            df_exploded = self.utilfunction.datediff_in_sec(df_exploded, "latest_inventory_transaction",
                                                            "last_transaction_time",
                                                            "untouched_ageing")
            df_exploded['slot_ageing'] = df_exploded['slot_ageing'] / 3600
            df_exploded['sku_ageing'] = df_exploded['sku_ageing'] / 3600
            df_exploded['untouched_ageing'] = df_exploded['untouched_ageing'] / 3600

            df_exploded.item_id = df_exploded.item_id.astype(int)

            df_exploded['season'] = df_exploded.sku_id
            df_exploded.season = df_exploded.season.astype(str)
            df_exploded = self.get_season(df_exploded)
            df_exploded['first_put_time'] = pd.to_datetime(df_exploded['first_put_time'])
            cron_run_at = datetime.now()
            cron_run_at = cron_run_at.replace(microsecond=0)
            df_exploded['cron_ran_at'] = cron_run_at
            if 'level_0' in df_exploded.columns:
                del df_exploded['level_0']
            df_exploded = self.utilfunction.reset_index(df_exploded)
            if 'level_0' in df_exploded.columns:
                del df_exploded['level_0']
            df_exploded = self.utilfunction.get_epoch_time(df_exploded, date1=cron_run_at)
            # group_df.to_csv(r'/home/ankush.j/Desktop/workname/big_data/group_df.csv', index=False, header=True)

            df_exploded['time'] = pd.to_datetime(df_exploded.time_ns)
            df_exploded = df_exploded.drop(columns=['time_ns', 'index'])
            # final_df=final_df[['time','rack_id','item_id']]
            # rackinfo=self.fetch_rack_info_from_tower()
            df_exploded = pd.merge(df_exploded, rackinfo, on=["rack_id"])
            df_exploded['racktype'] = df_exploded['rack_type']
            # df_exploded['racktype']='testing'
            # del df_exploded['rack_type']
            df_exploded.time = pd.to_datetime(df_exploded.time)
            df_exploded['untouched_ageing'] = df_exploded['untouched_ageing'].fillna(0)
            df_exploded['slot_ageing'] = df_exploded['slot_ageing'].fillna(0)
            df_exploded['sku_ageing'] = df_exploded['sku_ageing'].fillna(0)
            df_exploded['untouched_ageing'] = df_exploded['untouched_ageing'].astype(float)
            df_exploded['remaining_item_int'] = df_exploded['remaining_item_int'].astype(float)
            df_exploded['slot_ageing'] = df_exploded['slot_ageing'].astype(float)
            df_exploded['sku_ageing'] = df_exploded['sku_ageing'].astype(float)
            df_exploded['first_put_uom'] = df_exploded['first_put_uom'].astype(str)
            df_exploded['remaining_uom'] = df_exploded['remaining_uom'].astype(str)
            df_exploded['item_id'] = df_exploded['item_id'].astype(str)
            df_exploded['slot_id'] = df_exploded['slot_id'].astype(str)
            df_exploded['tpid'] = df_exploded['tpid'].astype(str)
            # final_df['racktype'] = final_df['racktype'].fillna('')
            df_exploded.latest_inventory_transaction = df_exploded.latest_inventory_transaction.apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%SZ'))
            df_exploded.first_put_time = df_exploded.first_put_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
            df_exploded.last_transaction_time = pd.to_datetime(df_exploded.last_transaction_time)
            df_exploded.last_transaction_time = df_exploded.last_transaction_time.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
            df_exploded.cron_ran_at = df_exploded.cron_ran_at.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
            #         # final_df['record_id'] = np.arange(1, (final_df.shape[0] + 1), 1)
            #         final_df['last_transaction_time'] = current_transaction_time
            if 'level_0' in df_exploded:
                del df_exploded['level_0']
            if 'Min_First_first_put_time' in df_exploded:
                del df_exploded['Min_First_first_put_time']
            df_exploded = df_exploded.set_index('time')

            if not df_exploded.empty:
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                     port=self.tenant_info["write_influx_port"])

                touched_item_count = df_exploded[df_exploded['sku_touched_untouched'] == 'touched']['item_id'].nunique()
                untouched_item_count = df_exploded[df_exploded['sku_touched_untouched'] == 'untouched']['item_id'].nunique()
                total_remaining_uom = df_exploded['remaining_item_int'].sum()
                touched_unique_ids = df_exploded[(df_exploded['slot_touched_untouched'] == 'touched') & (df_exploded['remaining_item_int'] > 0)][
                    'unique_id'].nunique()
                untouched_unique_ids = \
                df_exploded[(df_exploded['slot_touched_untouched'] == 'untouched') & (df_exploded['remaining_item_int'] > 0)][
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

                summary_df['host'] = self.tenant_info['host_name']
                summary_df['installation_id'] = self.tenant_info['installation_id']
                summary_df['host'] = summary_df['host'].astype(str)
                summary_df['installation_id'] = summary_df['installation_id'].astype(str)

                df_exploded['host'] = self.tenant_info['host_name']
                df_exploded['installation_id'] = self.tenant_info['installation_id']
                df_exploded['host'] = df_exploded['host'].astype(str)
                df_exploded['installation_id'] = df_exploded['installation_id'].astype(str)

                summary_df['cron_ran_at'] = summary_df['cron_ran_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
                summary_df.time = pd.to_datetime(summary_df.time)
                summary_df = summary_df.set_index('time')
                # try:
                #     self.client.query("drop measurement slots_ageing_elk")
                # except:
                #     pass


                self.write_client.writepoints(df_exploded, "slots_ageing_elk", db_name=self.tenant_info["out_db_name"],
                                              tag_columns=['rack_id'], dag_name=os.path.basename(__file__),
                                              site_name=self.tenant_info['Name'],retention_policy =rp_seven_days)

                self.write_client.writepoints(summary_df, "inventory_summary", db_name=self.tenant_info["out_db_name"],
                                              tag_columns=[], dag_name=os.path.basename(__file__),
                                              site_name=self.tenant_info['Name'],retention_policy =rp_seven_days)

            sl_util = slot_utilzation()
            sl_util.slot_utilzation_calc(tenant_info=tenant_info, end_date=self.end_date)
        return None


with DAG(
        'slot_ageing_using_elk',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='calculation of puts per rack face GM-44025',
        schedule_interval=timedelta(hours=7),
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
            if tenant['Active'] == "Y" and tenant['slot_ageing_elk'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                final_task = PythonOperator(
                    task_id='slot_ageing_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(slot_ageing().slot_ageing_final,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=9600),
                )

                warehouse_inventory_final_task = PythonOperator(
                    task_id='Warehouse_inventory_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(warehouse_inventory().warehouse_inventory,
                                                      tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600), )

    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='slot_ageing_final',
            provide_context=True,
            python_callable=functools.partial(slot_ageing().slot_ageing_final, tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )