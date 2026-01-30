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
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, MongoDBManager
from r2r_waittime_calculation import R2rWaittimeCalculation
from operator_working_time_summary import operator_working_time_summary
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

from config import (
    MongoDbServer
)
import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class MRUJourney:

    def mru_journey_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        self.CommonFunction = CommonFunction()

        try:
            self.end_date = datetime.now(timezone.utc)
            self.mru_journey_final1(self.end_date, **kwargs)
        except AirflowTaskTimeout as timeout_exception:
            raise timeout_exception
        except Exception as e:
            print(f"error:{e}")
            raise e

    def fetch_induction_time(self):
        q = f"select  rollcage_barcode,rollcage_type,bin_count_without_boxes from PlatformBusinessStats..rollcage_packing_box_induction_events where time >='{self.start_date}' and time <'{self.end_date}' and event_type ='CLOSE' order by time desc"
        print(q)
        df = pd.DataFrame(self.read_client.query(q).get_points())
        print(df.shape)
        df['event'] = 'induction_time'
        df['order_induction_time'] = df['time']
        return df

    def is_picking_done(self):
        q = f"select * from item_picked where time>='{self.start_date}' and time <'{self.end_date}' and value > 0 limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            return False
        return True

    def fetch_mru_flow(self):
        q = f"select * from mru_flow_events where time>='{self.start_date}' and time <'{self.end_date}' order by time desc"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        df['undock_requested_time'] = df.apply(lambda x: x['time'] if x['event'] == 'undock_requested' else None,
                                               axis=1)
        df['undock_complete_time'] = df.apply(lambda x: x['time'] if x['event'] == 'undock_complete' else None, axis=1)
        df['dock_complete_time'] = df.apply(lambda x: x['time'] if x['event'] == 'dock_complete' else None, axis=1)
        df['rack_id'] = df.apply(lambda x: x['rollcage_id'] if pd.isna(x['rack_id']) else x['rack_id'], axis=1)
        df['rollcage_type'] = df.apply(lambda x: x['rack_type'] if pd.isna(x['rollcage_type']) else x['rollcage_type'],
                                       axis=1)
        df.rename(columns={'rack_id': 'rollcage_barcode'}, inplace=True)
        return df

    def apply_ntile(self, df):
        df['ntile'] = 0
        Group = 1
        for x in df.index:
            if x > 0:
                if df['installation_id'][x] == df['installation_id'][x - 1] and df['pps_id'][x] == df['pps_id'][
                    x - 1] and df['group_id'][x] == df['group_id'][x - 1] and df['rollcage_barcode'][x] == \
                        df['rollcage_barcode'][x - 1] and df['event'][x] != 'dock_complete':
                    df["ntile"][x] = df["ntile"][x - 1]
                    df["rollcage_type"][x] = df["rollcage_type"][x - 1]
                else:
                    Group = Group + 1
                    df["ntile"][x] = Group
            else:
                df["ntile"][x] = Group
        return df

    def apply_ntile_x(self, df,max_val):
        df['ntile_x'] = max_val+1
        Group = max_val+1
        for x in df.index:
            if x > 0:
                if df['installation_id'][x] == df['installation_id'][x - 1] and df['pps_id'][x] == df['pps_id'][
                    x - 1] and df['group_id'][x] == df['group_id'][x - 1] and df['rollcage_barcode'][x] == \
                        df['rollcage_barcode'][x - 1] and df['event'][x] != 'dock_complete':
                    df["ntile"][x] = df["ntile"][x - 1]
                    df["rollcage_type"][x] = df["rollcage_type"][x - 1]
                else:
                    Group = Group + 1
                    df["ntile"][x] = Group
            else:
                df["ntile"][x] = Group
        return df
    def save_last_entry(self):
        filter = {"site": self.tenant_info['Name'], "table": "mru_journey"}
        new_last_run = {"last_run": self.end_date}
        self.CommonFunction.update_dag_last_run(filter, new_last_run)

    def save_last_entry2(self,min_start_time):
        filter = {"site": self.tenant_info['Name'], "table": "mru_journey"}
        new_last_run = {"last_run2": pd.to_datetime(min_start_time, utc=True).strftime("%Y-%m-%d %H:%M:%S.%f")}
        self.CommonFunction.update_dag_last_run(filter, new_last_run)

    def apply_ntile3(self, df):
        for x in df.index - 1:
            if x >= 0:
                if df['event'][x] == 'induction_time' and df['event'][x+1] == 'dock_complete' and df['rollcage_barcode'][x] == df['rollcage_barcode'][x + 1] and \
                        df['rollcage_type'][x] == df['rollcage_type'][x + 1] and pd.isna(df["ntile"][x]) and \
                        df["ntile"][x + 1] > 0:
                    df["ntile"][x] = df["ntile"][x + 1]
                    df["pps_id"][x] = df["pps_id"][x + 1]
                    df["dock_station_id"][x] = df["dock_station_id"][x + 1]
                    df["group_id"][x] = df["group_id"][x + 1]
                    df["host"][x] = df["host"][x + 1]
                    df["installation_id"][x] = df["installation_id"][x + 1]
        return df

    def fetch_item_picked(self):
        q = f"select bin_id, installation_id,order_id,pick_before_time,pps_id,bin_barcode as rollcage_barcode From item_picked where time >='{self.start_date}' and time<='{self.end_date}' and value >0"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        return df

    def fetch_order_events(self):
        q = f"select bin_id,external_service_request_id, installation_id,order_id,pick_before_time,pps_id,event_name From order_events where time >='{self.start_date}' and time<='{self.end_date}' and (event_name ='order_complete' )"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        df['order_creation_time'] = df.apply(
            lambda x: x['time'] if x['event_name'] == 'order_created' else None, axis=1)
        df['order_complete_time'] = df.apply(
            lambda x: x['time'] if x['event_name'] == 'order_complete' else None, axis=1)

        df = df.groupby(['installation_id','order_id'], as_index=False).agg(time=('time', 'first'), bin_id=('bin_id', 'first'),
                                                       external_service_request_id=('external_service_request_id', 'first'),
                                                       pick_before_time=('pick_before_time', 'first'),
                                                       pps_id=('pps_id', 'first'),
                                                       order_creation_time=('order_creation_time', 'first'),
                                                       order_complete_time=('order_complete_time', 'first')
                                                       )
        return df

    def fetch_order_events_created(self):
        q = f"select order_id,external_service_request_id, installation_id From order_events where time >='{self.back_date}' and time<='{self.end_date}' and (event_name ='order_created' )"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        df['order_creation_time'] = df['time']

        df = df.groupby(['installation_id','order_id'], as_index=False).agg(time=('time', 'first'),
                                                       external_service_request_id=('external_service_request_id', 'first'),
                                                       order_creation_time=('order_creation_time', 'first')
                                                       )
        return df

    def apply_ntile2(self, df):
        df['ntile2'] = 0
        Group = 1
        for x in df.index:
            if x > 0:
                if df['installation_id'][x] == df['installation_id'][x - 1] and df['order_id'][x] == df['order_id'][
                    x - 1]:
                    df["ntile2"][x] = df["ntile2"][x - 1]
                    # df["external_service_request_id"][x] = df["external_service_request_id"][x - 1]
                    # df["bin_id"][x - 1] = df["bin_id"][x]
                    # df["pps_id"][x - 1] = df["pps_id"][x]
                else:
                    Group = Group + 1
                    df["ntile2"][x] = Group
            else:
                df["ntile2"][x] = Group
        return df

    def fetch_pick_fsm(self):
        q = f"select installation_id,pps_id,bin_id from pick_fsm_events where time>='{self.start_date}' and time<='{self.end_date}' and event_name ='order_front_complete' "
        df = pd.DataFrame(self.read_client.query(q).get_points())
        df['event_name'] = 'order_release'
        df['pps_id'] = df['pps_id'].astype(int)
        df['bin_id'] = df['bin_id'].astype(int)
        return df

    def typecast_df_order_cols(self, df):
        df['bin_id'] = df['bin_id'].fillna(0)
        df['pps_id'] = df['pps_id'].fillna(0)
        df['bin_id'] = df['bin_id'].fillna(0).astype(int)
        df['pps_id'] = df['pps_id'].fillna(0).astype(int)
        return df

    def apply_ntile4(self, df):
        Group = 1
        for x in df.index:
            if x > 0:
                if df['installation_id'][x] == df['installation_id'][x - 1] and df['pps_id'][x] == df['pps_id'][
                    x - 1] and df['bin_id'][x] == df['bin_id'][x - 1] and pd.isna(df["ntile2"][x]):
                    df["ntile2"][x] = df["ntile2"][x - 1]
        return df

    def apply_ntile5(self, df):
        for x in df.index:
            if x > 0:
                if (df['installation_id'][x] == df['installation_id'][x - 1] and df['pps_id'][x] == df['pps_id'][
                    x - 1] and df['rollcage_barcode'][x] == df['rollcage_barcode'][x - 1]
                        and (pd.isna(df['rollcage_barcode_actual'][x - 1]))):
                    df["group_id"][x] = df["group_id"][x - 1]
                    df["fill_percent_float"][x] = df["fill_percent_float"][x - 1]
                    df["total_slots"][x] = df["total_slots"][x - 1]
                    df["undock_requested_time"][x] = df["undock_requested_time"][x - 1]
                    df["undock_complete_time"][x] = df["undock_complete_time"][x - 1]
                    df["dock_complete_time"][x] = df["dock_complete_time"][x - 1]
                    df["order_induction_time"][x] = df["order_induction_time"][x - 1]
                    df["dock_station_id"][x] = df["dock_station_id"][x - 1]
                    df["ntile"][x] = df["ntile"][x - 1]
        return df

    def apply_ntile6(self, df):
        for x in df.index:
            if x > 0:
                if (df['installation_id'][x] == df['installation_id'][x - 1] and df['pps_id'][x] == df['pps_id'][
                    x - 1] and df['rollcage_barcode'][x] == df['rollcage_barcode'][x - 1]
                        and (not pd.isna(df['rollcage_barcode_actual'][x - 1])) and (
                        not pd.isna(df['rollcage_barcode_actual'][x]))):
                    df["group_id"][x] = df["group_id"][x - 1]
                    df["fill_percent_float"][x] = df["fill_percent_float"][x - 1]
                    df["total_slots"][x] = df["total_slots"][x - 1]
                    df["undock_requested_time"][x] = df["undock_requested_time"][x - 1]
                    df["undock_complete_time"][x] = df["undock_complete_time"][x - 1]
                    df["dock_complete_time"][x] = df["dock_complete_time"][x - 1]
                    df["order_induction_time"][x] = df["order_induction_time"][x - 1]
                    df["dock_station_id"][x] = df["dock_station_id"][x - 1]
                    df["ntile"][x] = df["ntile"][x - 1]
        return df

    def write_data_influx(self, df):
        df['installation_id'] = self.tenant_info['installation_id']
        df['host'] = self.tenant_info['host_name']
        cols_to_del = ['ntile', 'ntile2']
        for col in cols_to_del:
            if col in df.columns:
                del df[col]
        df['time'] = pd.to_datetime(df['order_induction_time'], utc=True)
        df = df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "mru_journey", db_name=self.tenant_info["alteryx_out_db_name"],
                                      tag_columns=['dock_station_id', 'bin_id', 'installation_id'],
                                      dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

    def fetch_fetch_last_run2(self, table_name, site):
        last_run=self.start_date
        try:
            connection_string = MongoDbServer
            database_name = "GreyOrange"
            collection_name = 'last_dag_run'
            self.cls_mdb = MongoDBManager(connection_string, database_name, collection_name)
            query = {'site': site, 'table': table_name}
            data = self.cls_mdb.get_data(query, None)
            self.cls_mdb.close_connection()
            if data:
                df = pd.DataFrame(data)
                dag_last_run = df['last_run2'][0]
                if dag_last_run:
                    last_run = min(last_run, dag_last_run)
        except Exception as ex:
            pass
        return last_run

    def custom_get_start_date(self):
        self.start_date = self.client.get_start_date("mru_journey", self.tenant_info)
        print(f"Before start date:{self.start_date}")
        self.back_date = pd.to_datetime(self.start_date, utc=True) - timedelta(hours=48)
        self.start_date= self.fetch_fetch_last_run2("mru_journey", self.tenant_info['Name'])
        self.start_date = max(self.back_date,pd.to_datetime(self.start_date, utc=True))

        print(f"After start date{self.start_date}")
        self.back_date = (pd.to_datetime(self.start_date, utc=True) - timedelta(hours=48)).strftime(
            '%Y-%m-%dT%H:%M:%SZ')
        q = f"select * from mru_journey where time>'{self.back_date}' and time<='{self.start_date}' and undock_complete_time = '' order by time limit 1"
        df = pd.DataFrame(self.client.query(q).get_points())
        print(q)
        if not df.empty:
            self.start_date = df['time'].iloc[0]
        return pd.to_datetime(self.start_date, utc=True).strftime('%Y-%m-%dT%H:%M:%SZ')

    def mru_journey_final1(self, end_date, **kwargs):
        self.start_date = self.custom_get_start_date()
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
#        self.start_date = '2025-12-06T07:00:00.91Z'
        # self.end_date = '2025-12-02T08:00:00.91Z'
        print(self.start_date, self.end_date)

        if self.is_picking_done():

            df_induction_time = self.fetch_induction_time()
            df_mru_flow = self.fetch_mru_flow()
            df_mru_flow = df_mru_flow.sort_values(
                by=['installation_id', 'pps_id', 'group_id', 'rollcage_barcode', 'time'], \
                ascending=[True, True, True, True, True]).reset_index(drop=True)
            df_mru_flow = self.apply_ntile(df_mru_flow)
            df = pd.concat([df_induction_time, df_mru_flow], sort=False)
            df = df.sort_values(by=['rollcage_barcode', 'rollcage_type', 'time', 'ntile'], \
                                ascending=[True, True, True, True]).reset_index(drop=True)
            df = self.apply_ntile3(df)
            min_start_time=df[df['ntile'].isna()]['time'].min()
            if min_start_time:
                print(f"save temp data {min_start_time}")
                self.save_last_entry2(min_start_time)
            df =df[~df['ntile'].isna()]
            df = df.reset_index(drop=True)
            df['order_induction_time'] = df.apply(lambda x: x['time'] if x['event'] == 'induction_time' else None, axis=1)
            df = df.groupby(['ntile'], as_index=False).agg(time=('time', 'first'), pps_id=('pps_id', 'first'),
                                                           group_id=('group_id', 'first'),
                                                           fill_percent_float=('fill_percent_float', 'first'),
                                                           installation_id=('installation_id', 'first'),
                                                           total_slots=('total_slots_int', 'first'),
                                                           rollcage_barcode=('rollcage_barcode', 'first'),
                                                           undock_requested_time=('undock_requested_time', 'first'),
                                                           undock_complete_time=('undock_complete_time', 'first'),
                                                           dock_complete_time=('dock_complete_time', 'first'),
                                                           order_induction_time=('order_induction_time', 'first'),
                                                           dock_station_id=('dock_station_id', 'first')
                                                           )
            df = df[~df['order_induction_time'].isna()].reset_index(drop=True)
            df['time']= df.apply(lambda x: x['time'] if pd.isna(x['order_induction_time']) else x['order_induction_time'] ,axis=1)
            df_item = self.fetch_item_picked()
            df_order = self.fetch_order_events()
            df_order_created = self.fetch_order_events_created()
            df_order= pd.merge(df_item,df_order[['installation_id','order_id','order_complete_time']] ,on=['installation_id','order_id'],how ='left')
            df_order = pd.merge(df_order, df_order_created[['installation_id', 'order_id', 'external_service_request_id', 'order_creation_time']], on=['installation_id', 'order_id'], how='left')

            #df_order = pd.concat([df_order, df_item], sort=False)
            df_order = df_order.sort_values(by=['installation_id', 'order_id', 'time'], \
                                            ascending=[True, True, True]).reset_index(drop=True)

            df_order = self.apply_ntile2(df_order)
            df_order = self.typecast_df_order_cols(df_order)
            df_fsm_release = self.fetch_pick_fsm()
            temp_df = pd.concat([df_order, df_fsm_release], sort=False)
            temp_df = temp_df.sort_values(by=['installation_id', 'pps_id', 'bin_id', 'time'], \
                                          ascending=[True, True, True, True]).reset_index(drop=True)
            temp_df = self.apply_ntile4(temp_df)
            temp_df = temp_df.reset_index(drop=True)
            # temp_df['order_creation_time'] = temp_df.apply(
            #     lambda x: x['time'] if x['event_name'] == 'order_created' else None, axis=1)
            # temp_df['order_complete_time'] = temp_df.apply(
            #     lambda x: x['time'] if x['event_name'] == 'order_complete' else None, axis=1)
            temp_df['order_release_time'] = temp_df.apply(
                lambda x: x['time'] if x['event_name'] == 'order_release' else None, axis=1)
            temp_df = temp_df.groupby(['ntile2'], as_index=False).agg(time=('time', 'min'), time2=('time', 'max'),pps_id=('pps_id', 'min'),
                                                                      bin_id=('bin_id', 'min'),
                                                                      external_service_request_id=(
                                                                          'external_service_request_id', 'first'),
                                                                      installation_id=('installation_id', 'first'),
                                                                      order_id=('order_id', 'first'),
                                                                      pick_before_time=('pick_before_time', 'first'),
                                                                      rollcage_barcode=('rollcage_barcode', 'first'),
                                                                      order_creation_time=('order_creation_time',
                                                                                           'first'),
                                                                      order_complete_time=('order_complete_time',
                                                                                           'first'),
                                                                      order_release_time=('order_release_time', 'first')
                                                                      )
            temp_df['rollcage_barcode_actual'] = temp_df['rollcage_barcode']
            temp_df['order_complete_time']= temp_df.apply(lambda x: x['time2'] if pd.isna(x['order_complete_time']) and not pd.isna(x['order_release_time']) else x['order_complete_time'], axis=1)
            del temp_df['time2']
            temp_df['rollcage_barcode'] = temp_df['rollcage_barcode'].apply(
                lambda x: x[:-3] + x[-1] + x[-3:-1] if isinstance(x, str) and x[-1].isalpha() else x)
            temp_df['rollcage_barcode'] = temp_df['rollcage_barcode'].apply(lambda x: x[:-2] if not pd.isna(x) else x)
            #temp_df['rollcage_barcode'] = temp_df['rollcage_barcode'].apply(lambda x: x[:-1] if isinstance(x, str) and x[-1].isalpha() else x)
            final_df = pd.concat([df, temp_df], sort=False)
            final_df['pps_id'] = final_df['pps_id'].astype(int)
            final_df.time = pd.to_datetime(final_df.time)
            final_df = final_df.sort_values(by=['installation_id', 'pps_id', 'rollcage_barcode', 'time'], \
                                            ascending=[True, True, True, True]).reset_index(drop=True)
            final_df = self.apply_ntile5(final_df)
            final_df = self.apply_ntile6(final_df)
            final_df = final_df[~final_df['rollcage_barcode'].isna()].reset_index(drop=True)
            final_df = final_df[~final_df['order_induction_time'].isna()].reset_index(drop=True)
            final_df = final_df[~(~final_df['undock_complete_time'].isna() & final_df['rollcage_barcode_actual'].isna())].reset_index(drop=True)
            final_df['orderline_pick_time'] =final_df['time']
            final_df['orderline_pick_time'] = pd.to_datetime(final_df['orderline_pick_time'], utc=True,  errors='coerce')
            final_df = final_df[~(final_df['bin_id'].isna() & ~final_df['dock_complete_time'].isna())].reset_index(drop=True)
            final_df['order_completion_breached'] = final_df.apply(
                lambda x: 'false' if not pd.isna(x['pick_before_time']) and not pd.isna(x['order_complete_time']) and x['pick_before_time'][0:4]!= '9999' and pd.to_datetime(x['pick_before_time']) > pd.to_datetime(x['order_complete_time']) else (
                    'true' if not pd.isna(x['pick_before_time']) and not pd.isna(x['order_complete_time'])  and x['pick_before_time'][0:4]!= '9999' and pd.to_datetime(x['pick_before_time']) < pd.to_datetime(x['order_complete_time'])    else 'false'),
                axis=1)

            final_df['rollcage_undock_breached'] = final_df.apply(
                lambda x: 'false' if not pd.isna(x['pick_before_time']) and not pd.isna(x['undock_complete_time']) and x['pick_before_time'][0:4]!= '9999' and
                                     pd.to_datetime(x['pick_before_time']) > pd.to_datetime(x['undock_complete_time']) else (
                    'true' if not pd.isna(
                        x['pick_before_time']) and not pd.isna(x['undock_complete_time']) and x['pick_before_time'][0:4]!= '9999' and pd.to_datetime(x['pick_before_time']) < pd.to_datetime(x['undock_complete_time'])   else 'false'),
                axis=1)
            final_df['order_complete_undock_time_diff_in_sec']=None
            final_df = self.CommonFunction.datediff_in_sec(final_df, "undock_complete_time",
                                                                      "order_complete_time",
                                                                      "order_complete_undock_time_diff_in_sec")
            final_df['order_complete_date'] = final_df['order_complete_time'].apply(
                lambda x: pd.to_datetime(x).strftime('%Y-%m-%d') if not pd.isna(x) else x)

            str_cols = ['bin_id','dock_complete_time','dock_station_id','external_service_request_id','group_id','host','installation_id','order_complete_date',\
                        'order_complete_time','order_completion_breached','order_creation_time','order_id','order_induction_time','order_release_time','orderline_pick_time',\
                        'pick_before_time','pps_id','rollcage_barcode','rollcage_barcode_actual','rollcage_undock_breached','undock_complete_time','undock_requested_time']
            float_cols = ['total_slots','fill_percent_float','order_complete_undock_time_diff_in_sec']
            final_df[float_cols] = final_df[float_cols].fillna(0)

            final_df = self.CommonFunction.str_typecast(final_df, str_cols)
            final_df = self.CommonFunction.float_typecast(final_df, float_cols)
            self.write_data_influx(final_df)
        else:
            self.save_last_entry()

        return None


with DAG(
        'mru_journey',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='mru_journey dag created',
        schedule_interval='15 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(seconds=3600),
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['mru_journey'] == "Y" and (
                    tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='mru_journey_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(MRUJourney().mru_journey_final,
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
            task_id='mru_journey_final',
            provide_context=True,
            python_callable=functools.partial(MRUJourney().mru_journey_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

