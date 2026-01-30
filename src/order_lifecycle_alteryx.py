## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import numpy as np
import pandas as pd
import pytz
import time
from utils.DagIssueAlertUtils import log_failures, DbConnectionError


from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction, postgres_connection
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')
streaming=os.environ.get('streaming', "N")
TimeZone=os.environ.get('TimeZone', 'PDT')
Postgres_pf_user=os.environ.get('Postgres_pf_user', "altryx_read")
Postgres_pf_password=os.environ.get('Postgres_pf_password', "YLafmbfuji$@#45!")
Postgres_pf = os.environ.get('Postgres_pf', 'localhost')
Postgres_pf_port=os.environ.get('Postgres_pf_port', 5432)
sslrootcert=os.environ.get('sslrootcert', "")
sslcert=os.environ.get('sslcert', "")
sslkey=os.environ.get('sslkey', "")



## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class OrderLifecycle:

    def order_lifecycle_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.utilfunction = CommonFunction()
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.read_client_platformbusinessstats = InfluxData(host=self.tenant_info["influx_ip"],
                                                            port=self.tenant_info["influx_port"],
                                                            db="PlatformBusinessStats")
        # self.client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["write_influx_port"],db='GreyOrange')
        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')


        # self.end_date = datetime.now(timezone.utc)
        # self.order_lifecycle_final1(self.end_date, **kwargs)
        check_start_date = self.client.get_start_date("order_lifecycle_alteryx", self.tenant_info)
        check_end_date = datetime.now()
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q= f"select * from order_events where  event_name='order_complete' and time >= '{check_start_date}' and time < '{check_end_date}'  order by time desc limit 1"
        df_order_events = pd.DataFrame(self.client.query(q).get_points())
        if df_order_events.empty:
            self.end_date = datetime.now(timezone.utc)
            self.order_lifecycle_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("order_lifecycle_alteryx", '1d', self.tenant_info)
                if daterange.empty:
                    self.end_date = datetime.now(timezone.utc)
                    self.order_lifecycle_final1(self.end_date, **kwargs)
                else:
                    for i in daterange.index:
                        self.end_date = daterange['end_date'][i]
                        self.order_lifecycle_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def order_lifecycle_final1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("order_lifecycle_alteryx", self.tenant_info)
        self.start_date_5d = pd.to_datetime(self.start_date) - timedelta(days=5)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
        self.read_influxclient = self.read_client.influx_client
        self.end_date = end_date
        self.end_date = self.end_date.replace(minute=0,second=0)
        # self.start_date =self.start_date.astimezone(pytz.utc)
        # self.start_date_5d = self.start_date_5d.astimezone(pytz.utc)
        # self.end_date =self.end_date.astimezone(pytz.utc)
        # self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.start_date_5d = self.start_date_5d.strftime('%Y-%m-%dT%H:%M:%SZ')


        qry1=f"select * from order_events where  event_name='order_complete' and time >= '{self.start_date}' and time < '{self.end_date}'  and host = '{self.tenant_info['host_name']}' order by time desc"
        order_events_completed=pd.DataFrame(self.read_client.fetch_data(self.read_influxclient,qry1))
        qry2=f"select * from order_events where time >= '{self.start_date_5d}' and time < '{self.end_date}'  and host = '{self.tenant_info['host_name']}' order by time desc"
        #        df_main_qry2=pd.DataFrame(self.read_client.query(qry2).get_points())
        qry3=f"select order_id,pps_id,installation_id from streaming_order_events where  time >= '{self.start_date_5d}' and time < '{self.end_date}'  and host = '{self.tenant_info['host_name']}' order by time desc"
        qry4 = f"select order_id_field from mtu_data_platform where  time >= '{self.start_date_5d}' and time < '{self.end_date}'  and order_allocated_type ='extracted' order by time desc"
        #        df_main_qry3=pd.DataFrame(self.read_client.query(qry3).get_points())
        if not (order_events_completed.empty or 'error'  in order_events_completed.columns):
            order_events_completed['time'] = pd.to_datetime(order_events_completed['time'])
            order_events_completed['time'] = order_events_completed['time'].apply(lambda x: x.to_pydatetime())
            df = self.utilfunction.create_time_series_data2(pd.to_datetime(self.start_date_5d).strftime('%Y-%m-%dT%H:%M:%S'), pd.to_datetime(self.end_date).strftime('%Y-%m-%dT%H:%M:%S'), '30min')
            df['qry'] = df.apply(
                lambda x: "select * from order_events where time>='" + str(x['interval_start']) + "' and time<'" + str(
                    x['interval_end']) + "' order by time desc", axis=1)

            ideal_dataset = pd.DataFrame(columns=['time', 'bin_id', 'bintags', 'breach_state', 'breached', 'critical',
                                                  'event_name', 'external_service_request_id', 'host', 'installation_id',
                                                  'item_id', 'order_flow_name', 'order_id', 'pick_after_time',
                                                  'pick_before_time', 'picked_quantity', 'pps_id', 'ppsbin_id',
                                                  'seat_name', 'tag_order_flow_name', 'value','station_type','storage_type'])

            for i in range(len(df)):
                order_events_all5days =self.read_client.fetch_data(self.read_influxclient,df['qry'][i])
                if 'error' in order_events_all5days[0].keys():
                    print("error")
                    print(order_events_all5days)
                    pass
                else:
                    order_events_all5days = pd.DataFrame(order_events_all5days)
                    ideal_dataset = ideal_dataset.append(order_events_all5days, ignore_index=False)


            order_events_all5days = ideal_dataset
            rtp_site= True
            if 'station_type' in order_events_completed:
                rtp_site = False
            order_events_all5days['station_type'] = order_events_all5days.apply(lambda x: 'rtp' if 'station_type' not in x or pd.isna(x['station_type']) else x['station_type'], axis=1)
            order_events_all5days['storage_type'] = order_events_all5days.apply(lambda x: 'msu' if 'storage_type' not in x or pd.isna(x['storage_type']) else x['storage_type'], axis=1)

            order_events_all5days['time'] = pd.to_datetime(order_events_all5days['time'])
            order_events_all5days['time'] = order_events_all5days['time'].apply(lambda x: x.to_pydatetime())

            replacestrings = {None: '2001-01-01T01:01:01Z', 'undefined': '2001-01-01T01:01:01Z'}
            order_events_all5days["pick_before_time"].replace(replacestrings, inplace=True)
            order_events_all5days["pick_after_time"].replace(replacestrings, inplace=True)
            order_events_all5days.loc[order_events_all5days["pick_before_time"].str.contains("9999"), "pick_before_time"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            order_events_all5days.loc[order_events_all5days["pick_after_time"].str.contains("9999"), "pick_after_time"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

            order_events_all5days['pick_before_time'] = pd.to_datetime(order_events_all5days['pick_before_time'])
            order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'].apply(
                lambda x: x.to_pydatetime())
            order_events_all5days['pick_after_time'] = pd.to_datetime(order_events_all5days['pick_after_time'])
            order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'].apply(
                lambda x: x.to_pydatetime())
            orders_df = pd.DataFrame(
                columns=['time', 'order_id', 'pps_id', 'installation_id', 'breached_state', 'critical', 'event_name'])
            try:
                streaming_orders = self.read_client.fetch_data(self.read_influxclient,qry3)
                streaming_orders = pd.DataFrame(streaming_orders)
                if not (streaming_orders.empty or 'error'  in streaming_orders[0].keys()):
                    streaming_orders['time'] = pd.to_datetime(streaming_orders['time'])
                    streaming_orders['time'] = streaming_orders['time'].apply(lambda x: x.to_pydatetime())
                    streaming_orders['breach_state'] = 'non_breach'
                    streaming_orders['critical'] = 'false'
                    streaming_orders['event_name'] = 'order_created'
                else:
                    streaming_orders = orders_df
            except:
                streaming_orders = orders_df

            order_events_completed = pd.DataFrame(
                order_events_completed.groupby('order_id')['time'].count().reset_index().reset_index())
            order_events_completed.rename(columns={'index': 'group'}, inplace=True)
            order_events_completed.drop(['time'], axis=1, inplace=True)

            order_events_all5days = pd.merge(order_events_all5days, order_events_completed, on='order_id')

            streaming_all5days = pd.merge(streaming_orders, order_events_all5days[['order_id']], on='order_id')
            streaming_all5days.sort_values('time', ascending=True, inplace=True)
            streaming_all5days.drop_duplicates(inplace=True)

            order_events_all5days = pd.concat([streaming_all5days, order_events_all5days])
            order_events_all5days = pd.merge(order_events_all5days, order_events_completed, on='order_id')
            order_events_all5days.drop(['group_x', 'tag_order_flow_name'], axis=1, inplace=True)
            order_events_all5days.rename(columns={'group_y': 'group'}, inplace=True)

            di = {'order_created': 1, 'order_tasked': 2, 'item_picked': 3, 'order_complete': 4,
                  'order_temporary_unfulfillable': 5,
                  'inventory_awaited': 6, 'order_recalculated_for_deadlock': 7, 'temporary_unfulfillable': 8,
                  'order_unfulfillable': 9}
            order_events_all5days["event_name_unique"] = order_events_all5days["event_name"]
            order_events_all5days["event_name_unique"].replace(di, inplace=True)

            order_events_all5days = order_events_all5days[
                ~((order_events_all5days['picked_quantity'] == '0') & (
                            order_events_all5days['event_name'] == "item_picked"))]
            order_events_all5days.sort_values(by=['order_id', 'time'], ascending=True, inplace=True)
            dataset1 = order_events_all5days.copy()
            if not order_events_all5days.empty:
                order_events_all5days['order_created'] = order_events_all5days.apply(
                    lambda x: x['time'] if x['event_name_unique'] == 1 else np.nan, axis=1)
                order_events_all5days['order_tasked'] = order_events_all5days.apply(
                    lambda x: x['time'] if x['event_name_unique'] == 2 else np.nan, axis=1)
                order_events_all5days['item_picked'] = order_events_all5days.apply(
                    lambda x: x['time'] if x['event_name_unique'] == 3 else np.nan, axis=1)
                order_events_all5days['order_complete'] = order_events_all5days.apply(
                    lambda x: x['time'] if x['event_name_unique'] == 4 else np.nan, axis=1)
                order_events_all5days['PBT'] = order_events_all5days['pick_before_time']
                order_events_all5days['breached'] = order_events_all5days.apply(
                    lambda x: 'false' if x['event_name'] == 'order_complete' and x['PBT'] > x['order_complete'] else (
                        'true' if x['event_name'] == 'order_complete' and x['PBT'] < x['order_complete'] else np.nan), axis=1)
                dataset2 = order_events_all5days.copy()

                order_events_all5days['picked_quantity'] = order_events_all5days['picked_quantity'].replace(np.nan, 0)
                order_events_all5days['picked_quantity'] = order_events_all5days['picked_quantity'].astype('int64')

                order_events_all5days['bin_id'] = order_events_all5days['bin_id'].replace(np.nan, 0)
                order_events_all5days['bin_id'] = order_events_all5days['bin_id'].astype('int64')

                df1 = order_events_all5days.groupby(
                    ['installation_id', 'order_id', 'group', 'event_name_unique', 'event_name', 'bin_id','station_type','storage_type']).agg(
                    {'order_created': 'last', 'order_tasked': 'last', 'item_picked': 'first', 'order_complete': 'last',
                    'PBT': 'max',
                    'picked_quantity': 'sum', 'breached': 'last'}).reset_index()
                df2 = pd.DataFrame(order_events_all5days.groupby(
                    ['installation_id', 'order_id', 'group', 'event_name_unique', 'event_name', 'bin_id', 'pps_id','station_type','storage_type'])[
                                    'pps_id'].count())
                df2.rename(columns={'pps_id': 'count'}, inplace=True)
                df2 = df2.reset_index()
                df2 = df2[df2['event_name'] == 'item_picked']
                df2 = df2[['installation_id', 'order_id', 'group', 'pps_id']]

                order_events_all5days = pd.merge(df1, df2, on=['installation_id', 'order_id', 'group'])

                # if not dataset2.empty:
                streamingpart = dataset2.groupby(['installation_id', 'order_id', 'group', 'event_name', 'pps_id','station_type','storage_type']).agg(
                {'order_created': 'last'}).reset_index()
                streamingpart = streamingpart.pivot(index=['installation_id', 'order_id', 'group', 'pps_id','station_type','storage_type'], columns=['event_name'],
                                                    values=['order_created'])
                streamingpart = streamingpart['order_created'].reset_index()
                if 'order_created' in streamingpart.columns:
                    streamingpart = streamingpart[streamingpart['order_created'].isna()]
                streamingpart['event_name'] = 'order_created'
                streamingpart['event_name_unique'] = 1
                streamingpart['order_created'] = self.start_date_5d
                order_events_all5days = pd.concat([streamingpart, order_events_all5days], ignore_index=True)

                order_events_all5days = order_events_all5days[
                    ['installation_id', 'order_id', 'group', 'event_name_unique', 'event_name', 'order_created', 'item_picked',
                    'order_complete', 'order_tasked', 'PBT', 'picked_quantity', 'pps_id', 'bin_id', 'breached','station_type','storage_type']]

                replacestrings = {None: '2001-01-01 01:01:01', 'undefined': '2001-01-01 01:01:01'}
                order_events_all5days["order_created"].replace(replacestrings, inplace=True)
                order_events_all5days['order_created'] = pd.to_datetime(order_events_all5days['order_created'], utc=True)
                order_events_all5days['breached'] = order_events_all5days.groupby('group')['breached'].shift(-3, axis=0)

                otu = order_events_all5days[order_events_all5days['event_name'] == 'order_temporary_unfulfillable']
                otu = otu[['installation_id', 'order_id', 'group', 'event_name']]
                otu['order_temporary_unfulfillable'] = 'true'

                oia = order_events_all5days[order_events_all5days['event_name'] == 'order_inventory_awaited']
                oia = oia[['installation_id', 'order_id', 'group', 'event_name']]
                oia['order_inventory_awaited'] = 'true'

                orfd = order_events_all5days[order_events_all5days['event_name'] == 'order_recalculated_for_deadlock']
                orfd = oia[['installation_id', 'order_id', 'group', 'event_name']]
                orfd['order_recalculated_for_deadlock'] = 'true'

                tu = order_events_all5days[order_events_all5days['event_name'] == 'temporary_unfulfillable']
                tu = oia[['installation_id', 'order_id', 'group', 'event_name']]
                tu['temporary_unfulfillable'] = 'true'

                ou = order_events_all5days[order_events_all5days['event_name'] == 'order_unfulfillable']
                ou = oia[['installation_id', 'order_id', 'group', 'event_name']]
                ou['order_unfulfillable'] = 'true'

                order_events_all5days = pd.merge(order_events_all5days, otu,
                                                on=['installation_id', 'order_id', 'group', 'event_name'],
                                                how='left')
                order_events_all5days['order_temporary_unfulfillable'] = order_events_all5days[
                    'order_temporary_unfulfillable'].fillna(
                    'false')

                order_events_all5days = pd.merge(order_events_all5days, oia,
                                                on=['installation_id', 'order_id', 'group', 'event_name'],
                                                how='left')
                order_events_all5days['order_inventory_awaited'] = order_events_all5days['order_inventory_awaited'].fillna(
                    'false')

                order_events_all5days = pd.merge(order_events_all5days, orfd,
                                                on=['installation_id', 'order_id', 'group', 'event_name'],
                                                how='left')
                order_events_all5days['order_recalculated_for_deadlock'] = order_events_all5days[
                    'order_recalculated_for_deadlock'].fillna('false')

                order_events_all5days = pd.merge(order_events_all5days, tu,
                                                on=['installation_id', 'order_id', 'group', 'event_name'],
                                                how='left')
                order_events_all5days['temporary_unfulfillable'] = order_events_all5days['temporary_unfulfillable'].fillna(
                    'false')

                order_events_all5days = pd.merge(order_events_all5days, ou,
                                                on=['installation_id', 'order_id', 'group', 'event_name'],
                                                how='left')
                order_events_all5days['order_unfulfillable'] = order_events_all5days['order_unfulfillable'].fillna('false')

                order_events_all5days['picked_quantity'] = order_events_all5days.groupby('group')['picked_quantity'].shift(-2,
                                                                                                                        axis=0)

                order_events_all5days['order_tasked'] = order_events_all5days.groupby('group')['order_tasked'].shift(-1, axis=0)
                order_events_all5days['item_picked'] = order_events_all5days.groupby('group')['item_picked'].shift(-2, axis=0)

                order_events_all5days['order_complete'] = order_events_all5days.groupby('group')['order_complete'].shift(-3,
                                                                                                                        axis=0)
                order_events_all5days['bin_id'] = order_events_all5days.groupby('group')['bin_id'].shift(-3, axis=0)

                order_events_all5days['PBT'] = order_events_all5days['PBT'].astype('object')

                order_events_all5days['pbt1'] = order_events_all5days.groupby('group')['PBT'].shift(-3, axis=0)
                order_events_all5days['pbt2'] = order_events_all5days.groupby('group')['PBT'].shift(-2, axis=0)
                order_events_all5days['pbt3'] = order_events_all5days.groupby('group')['PBT'].shift(-1, axis=0)

                order_events_all5days['pbt1'] = order_events_all5days['pbt1'].astype('object')
                order_events_all5days['pbt2'] = order_events_all5days['pbt2'].astype('object')
                order_events_all5days['pbt3'] = order_events_all5days['pbt3'].astype('object')

                replacestrings = {None: '2001-01-01 01:01:01', 'undefined': '2001-01-01 01:01:01'}
                order_events_all5days["pbt1"].replace(replacestrings, inplace=True)
                order_events_all5days["pbt2"].replace(replacestrings, inplace=True)
                order_events_all5days["pbt3"].replace(replacestrings, inplace=True)

                if not order_events_all5days.empty:
                    order_events_all5days['PBT'] = order_events_all5days.apply(
                        lambda x: x['pbt1'] if x['pbt1'] != '2001-01-01 01:01:01' else (
                            x['pbt2'] if x['pbt2'] != '2001-01-01 01:01:01' else x['pbt3']), axis=1)
                order_events_all5days.drop(['pbt1', 'pbt2', 'pbt3'], axis=1, inplace=True)

                order_events_all5days = order_events_all5days[order_events_all5days['event_name_unique'] == 1]

                replacestrings = {None: '2001-01-01 01:01:01', 'undefined': '2001-01-01 01:01:01'}
                order_events_all5days["PBT"].replace(replacestrings, inplace=True)
                order_events_all5days['PBT'] = pd.to_datetime(order_events_all5days['PBT'], utc=True)

                order_events_all5days["creation_to_tasked"]=None
                order_events_all5days["tasked_to_first_item_picked"] = None
                order_events_all5days["first_item_picked_to_order_complete"] = None
                order_events_all5days["time_to_pick"] = None
                order_events_all5days["pbt_to_completion"] = None
                order_events_all5days["pbt_to_task"] = None
                order_events_all5days["created_to_completed"] = None

                order_events_all5days = self.utilfunction.datediff_in_sec(order_events_all5days, "order_tasked", "order_created",
                                                    "creation_to_tasked")
                # order_events_all5days['creation_to_tasked'] = (
                #         order_events_all5days['order_tasked'] - order_events_all5days['order_created'])
                order_events_all5days = self.utilfunction.datediff_in_sec(order_events_all5days, "item_picked", "order_tasked",
                                                    "tasked_to_first_item_picked")

                # order_events_all5days['tasked_to_first_item_picked'] = (
                #         order_events_all5days['item_picked'] - order_events_all5days['order_tasked'])
                order_events_all5days = self.utilfunction.datediff_in_sec(order_events_all5days, "order_complete", "item_picked",
                                                    "first_item_picked_to_order_complete")

                # order_events_all5days['first_item_picked_to_order_complete'] = (
                #         order_events_all5days['order_complete'] - order_events_all5days['item_picked'])
                order_events_all5days = self.utilfunction.datediff_in_sec(order_events_all5days, "PBT", "order_created",
                                                    "time_to_pick")
                # order_events_all5days['time_to_pick'] = (order_events_all5days['PBT'] - order_events_all5days['order_created'])
                order_events_all5days = self.utilfunction.datediff_in_sec(order_events_all5days, "PBT", "order_complete",
                                                    "pbt_to_completion")

                # order_events_all5days['pbt_to_completion'] = (
                #             order_events_all5days['PBT'] - order_events_all5days['order_complete'])
                order_events_all5days = self.utilfunction.datediff_in_sec(order_events_all5days, "PBT", "order_tasked",
                                                    "pbt_to_task")

                # order_events_all5days['pbt_to_task'] = (order_events_all5days['PBT'] - order_events_all5days['order_tasked'])
                order_events_all5days = self.utilfunction.datediff_in_sec(order_events_all5days, "order_complete", "order_created",
                                                    "created_to_completed")
                #
                # order_events_all5days['created_to_completed'] = (
                #         order_events_all5days['order_complete'] - order_events_all5days['order_created'])

                # order_events_all5days['creation_to_tasked'] = order_events_all5days['creation_to_tasked'].apply(
                #     lambda x: x.total_seconds())
                # order_events_all5days['tasked_to_first_item_picked'] = order_events_all5days[
                #     'tasked_to_first_item_picked'].apply(
                #     lambda x: x.total_seconds())
                # order_events_all5days['first_item_picked_to_order_complete'] = order_events_all5days[
                #     'first_item_picked_to_order_complete'].apply(lambda x: x.total_seconds())
                # order_events_all5days['time_to_pick'] = order_events_all5days['time_to_pick'].apply(lambda x: x.total_seconds())
                # order_events_all5days['pbt_to_completion'] = order_events_all5days['pbt_to_completion'].apply(
                #     lambda x: x.total_seconds())
                order_events_all5days['pbt_to_creation'] = order_events_all5days['time_to_pick']
                # order_events_all5days['pbt_to_task'] = order_events_all5days['pbt_to_task'].apply(lambda x: x.total_seconds())
                # order_events_all5days['created_to_completed'] = order_events_all5days['created_to_completed'].apply(
                #     lambda x: x.total_seconds())

                order_events_all5days['total_time'] = order_events_all5days['creation_to_tasked'] + order_events_all5days[
                    'tasked_to_first_item_picked'] + order_events_all5days['first_item_picked_to_order_complete']
                if  not order_events_all5days.empty:
                    order_events_all5days['order_temporary_unfulfillable'] = order_events_all5days.apply(
                        lambda x: x['temporary_unfulfillable'] if x['temporary_unfulfillable'] == 'true' else x[
                            'order_temporary_unfulfillable'], axis=1)
                order_events_all5days.drop(['temporary_unfulfillable'], axis=1, inplace=True)

                dataset2.sort_values(by="time", ascending=True, inplace=True)
                add_pbt_and_pat = dataset2[dataset2['event_name_unique'] == 4]
                add_pbt_and_pat = add_pbt_and_pat.groupby(['order_id','installation_id']).agg(
                    {'pick_before_time': 'first', 'pick_after_time': 'first', 'order_flow_name': 'first', 'time': 'first',
                    'bintags': 'first'}).reset_index()

                breached_order_metrics = dataset2[
                    (dataset2['event_name_unique'] == 4) | (dataset2['event_name_unique'] == 1) | (
                                dataset2['event_name_unique'] == 2)]
                breached_order_metrics = breached_order_metrics.groupby(
                    ['installation_id', 'order_id', 'group', 'event_name', 'event_name_unique']).agg(
                    {'breach_state': 'last'}).reset_index()
                breached_order_metrics = breached_order_metrics[['installation_id', 'order_id', 'event_name', 'breach_state']]
                breached_order_metrics =breached_order_metrics.reset_index()
                breached_order_metrics = breached_order_metrics.pivot(index=['installation_id', 'order_id'], columns=['event_name'],
                                                                    values=['breach_state'])
                breached_order_metrics = breached_order_metrics.add_prefix('breached_')
                breached_order_metrics = breached_order_metrics['breached_breach_state'].reset_index()

                breached_order_metrics2 = dataset1[(dataset1['event_name_unique'] == 4) | (dataset1['event_name_unique'] == 2)]
                if breached_order_metrics2.empty:
                    return
                breached_order_metrics2 = breached_order_metrics2.groupby(
                    ['installation_id', 'order_id', 'group', 'event_name', 'event_name_unique']).agg(
                    {'breached': 'last'}).reset_index()
                breached_order_metrics2 = breached_order_metrics2[['installation_id', 'order_id', 'event_name', 'breached']]
                breached_order_metrics2 = breached_order_metrics2.pivot(index=['installation_id', 'order_id'], columns=['event_name'],
                                                                        values=['breached'])
                breached_order_metrics2 = breached_order_metrics2['breached'].reset_index()
                dic = {'order_complete': 'breached_completed',
                    'order_tasked': 'breached_tasked'}
                breached_order_metrics2.rename(columns=dic, inplace=True)
                if 'breached_tasked' not in breached_order_metrics2:
                    breached_order_metrics2['breached_tasked']='false'
                if 'breached_completed' not in breached_order_metrics2:
                    breached_order_metrics2['breached_completed']='false'

    #            if 'breached_order_complete' not in breached_order_metrics2:
    #                breached_order_metrics2['breached_order_complete']='non_breach'

                breached_order_info = pd.merge(breached_order_metrics, breached_order_metrics2, on=['order_id','installation_id'] )
                breached_order_info = pd.merge(breached_order_info, add_pbt_and_pat, on=['order_id','installation_id'])
                order_events_all5days = pd.merge(order_events_all5days, breached_order_info, on=['order_id','installation_id'])

                order_events_all5days.drop(['event_name', 'event_name_unique', 'group', 'item_picked', 'order_complete'],
                                        axis=1,
                                        inplace=True)

                dic = {'order_created': 'order_created_time',
                    'order_tasked': 'order_tasked_time'}

                order_events_all5days.rename(columns=dic, inplace=True)

                ext_req_id = pd.DataFrame(order_events_all5days.groupby(['order_id','installation_id'])['order_created_time'].count()).reset_index()
                ext_req_id = ext_req_id[['order_id','installation_id']]
                ext_req_id['order_id'] = ext_req_id['order_id'].apply(lambda x: 'id=' + str(x))
                ext_req_id['contains_group'] = ext_req_id['order_id'].apply(lambda x: 1 if 'group' in x else 0)
                ext_req_id_no_gr = ext_req_id[ext_req_id['contains_group'] != 1]
                countofrec = len(ext_req_id_no_gr['order_id'])

                # concat_string = ' or '.join(list(ext_req_id_no_gr['order_id']))
                if countofrec == 0:
                    data_ext_req_id = pd.DataFrame(columns=['order_id', 'external_service_request_id'])
                    data_ext_req_id['order_id'] = data_ext_req_id['order_id'].astype('object')
                else:
                    l= 0
                    data_ext_req_id=[]
                    while(l<=countofrec):
                        concat_string = ' or '.join(list(ext_req_id_no_gr['order_id'])[l:l+500])
                        if rtp_site:
                            qry = "select id,external_service_request_id    from   service_request where " + concat_string
                        else:
                            qry = "select id,external_service_request_id ,station_type,storage_type from service_request where " + concat_string

                        if self.tenant_info['Postgres_pf_user'] !="":
                            self.postgres_conn = postgres_connection(database='platform_srms', user=self.tenant_info['Postgres_pf_user'], \
                                                                    sslrootcert=self.tenant_info['sslrootcert'],
                                                                    sslcert=self.tenant_info['sslcert'], \
                                                                    sslkey=self.tenant_info['sslkey'],
                                                                    host=self.tenant_info['Postgres_pf'], \
                                                                    port=self.tenant_info['Postgres_pf_port'],
                                                                    password=self.tenant_info['Postgres_pf_password'],
                                                                    dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                            temp_data_ext_req_id = self.postgres_conn.fetch_postgres_data_in_chunk(qry)
                            self.postgres_conn.close()
                            data_ext_req_id=data_ext_req_id+temp_data_ext_req_id
                        l=l+500
                    if rtp_site:
                        data_ext_req_id = pd.DataFrame(data_ext_req_id, columns=['order_id', 'external_service_request_id'])
                        data_ext_req_id['station_type'] ='rtp'
                        data_ext_req_id['storage_type'] = 'msu'
                    else:
                        data_ext_req_id = pd.DataFrame(data_ext_req_id,
                                                       columns=['order_id', 'external_service_request_id',
                                                                'station_type', 'storage_type'])
                    data_ext_req_id['order_id'] = data_ext_req_id['order_id'].astype('str')

                order_events_all5days['order_id'] = order_events_all5days['order_id'].astype('str')
                order_events_all5days = pd.merge(order_events_all5days, data_ext_req_id, on='order_id', how='left')

                order_events_all5days['external_service_request_id'] = order_events_all5days[
                    'external_service_request_id'].fillna('')
                (h,m) = self.utilfunction.get_timezoneinfodata(timez=self.tenant_info["TimeZone"])
                order_events_all5days['mtu_extraction_breached'] = 'non-breached'
                try:
                    platformbusinessstats_orders = pd.DataFrame(
                        self.read_client_platformbusinessstats.query(qry4).get_points())
                    if not platformbusinessstats_orders.empty:
                        platformbusinessstats_orders.rename(columns={'order_id_field': 'order_id'}, inplace=True)
                        order_events_all5days = pd.merge(order_events_all5days, platformbusinessstats_orders,
                                                         on='order_id', how='left' )
                        order_events_all5days['mtu_extraction_breached'] = order_events_all5days.apply(
                            lambda x: 'breached' if not pd.isna(x['time_y']) and x['pick_before_time'] < pd.to_datetime(x['time_y'],utc=True) else 'non-breached', axis=1)
                        del order_events_all5days['time_y']
                        order_events_all5days.rename(columns={'time_x': 'time'}, inplace=True)
                except:
                    if 'time_x' in order_events_all5days.columns:
                        order_events_all5days.rename(columns={'time_x': 'time'}, inplace=True)
                    if 'time_y' in order_events_all5days.columns:
                        del order_events_all5days['time_y']
                    pass
                order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'] + timedelta(hours=h)
                order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'] + timedelta(minutes=m)

                order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'] + timedelta(hours=h)
                order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'] + timedelta(minutes=m)
                order_events_all5days['pps_id'] = order_events_all5days['pps_id'].fillna('0')
                dateconversion= lambda x:pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S') if (not pd.isna(x)) else x
                order_events_all5days['order_created_time']= order_events_all5days['order_created_time'].apply(dateconversion)
                order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'].apply(dateconversion)
                order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'].apply(dateconversion)
                order_events_all5days['order_tasked_time'] = order_events_all5days['order_tasked_time'].apply(dateconversion)
                order_events_all5days['PBT'] = order_events_all5days['PBT'].apply(dateconversion)

                order_events_all5days['order_created_time'] = order_events_all5days['order_created_time'].astype('object')
                order_events_all5days['order_tasked_time'] = order_events_all5days['order_tasked_time'].astype('object')
                order_events_all5days['PBT'] = order_events_all5days['PBT'].astype('object')
                order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'].astype('object')
                order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'].astype('object')
                order_events_all5days['bin_id'] = order_events_all5days['bin_id'].astype('object')
                order_events_all5days['breached'] = order_events_all5days['breached'].astype('string')
                order_events_all5days['order_temporary_unfulfillable'] = order_events_all5days['order_temporary_unfulfillable'].astype('string')
                order_events_all5days['order_inventory_awaited'] = order_events_all5days['order_inventory_awaited'].astype('string')
                order_events_all5days['order_recalculated_for_deadlock'] = order_events_all5days['order_recalculated_for_deadlock'].astype('string')
                order_events_all5days['order_unfulfillable'] = order_events_all5days['order_unfulfillable'].astype('string')
                order_events_all5days['breached_completed'] = order_events_all5days['breached_completed'].astype('string')
                order_events_all5days['breached_tasked'] = order_events_all5days['breached_tasked'].astype('string')
                order_events_all5days['creation_to_tasked']=order_events_all5days['creation_to_tasked'].fillna(0)
                order_events_all5days['creation_to_tasked'] = order_events_all5days['creation_to_tasked'].astype('int')
                order_events_all5days['first_item_picked_to_order_complete'] = order_events_all5days['first_item_picked_to_order_complete'].fillna(0)
                order_events_all5days['first_item_picked_to_order_complete'] = order_events_all5days['first_item_picked_to_order_complete'].astype('int')
                order_events_all5days['pbt_to_completion'] = order_events_all5days['pbt_to_completion'].fillna(0)
                order_events_all5days['pbt_to_completion'] = order_events_all5days['pbt_to_completion'].astype('int')
                order_events_all5days['tasked_to_first_item_picked'] = order_events_all5days['tasked_to_first_item_picked'].fillna(0)
                order_events_all5days['tasked_to_first_item_picked'] = order_events_all5days['tasked_to_first_item_picked'].astype('int')
                order_events_all5days['time_to_pick'] = order_events_all5days['time_to_pick'].fillna(0)
                order_events_all5days['time_to_pick'] = order_events_all5days['time_to_pick'].astype('int')
                order_events_all5days.rename(columns={'picked_quantity': 'total_picked_quantity'}, inplace=True)
                order_events_all5days['total_picked_quantity'] = order_events_all5days['total_picked_quantity'].fillna(0)
                order_events_all5days['total_picked_quantity'] = order_events_all5days['total_picked_quantity'].astype('int')
                order_events_all5days['total_time'] = order_events_all5days['total_time'].fillna(0)
                order_events_all5days['total_time'] = order_events_all5days['total_time'].astype('int')
                order_events_all5days['external_service_request_id']=order_events_all5days['external_service_request_id'].astype('string')
                order_events_all5days['external_service_request_id'] = order_events_all5days[
                    'external_service_request_id'].fillna('')
                order_events_all5days['breached_completed'] = order_events_all5days['breached_completed'].astype('string')
                order_events_all5days['breached'] = order_events_all5days['breached'].apply(lambda x: str(x))
                order_events_all5days['breached_completed'] = order_events_all5days['breached_completed'].apply(lambda x: str(x))
                order_events_all5days['order_temporary_unfulfillable'] = order_events_all5days['order_temporary_unfulfillable'].apply(lambda x: str(x))
                order_events_all5days['order_inventory_awaited'] = order_events_all5days['order_inventory_awaited'].apply(lambda x: str(x))
                order_events_all5days['order_recalculated_for_deadlock'] = order_events_all5days['order_recalculated_for_deadlock'].apply(lambda x: str(x))
                order_events_all5days['order_unfulfillable'] = order_events_all5days['order_unfulfillable'].apply(lambda x: str(x))
                order_events_all5days['breached_completed'] = order_events_all5days['breached_completed'].apply(lambda x: str(x))
                order_events_all5days['breached_tasked'] = order_events_all5days['breached_tasked'].apply(lambda x: str(x))
                order_events_all5days['pbt_to_task'] = order_events_all5days['pbt_to_task'].apply(lambda x: float(x) if not pd.isna(x) else 0.0)
                order_events_all5days['created_to_completed'] = order_events_all5days['created_to_completed'].apply(lambda x: float(x) if not pd.isna(x) else 0.0)
                order_events_all5days['bin_id'] = order_events_all5days['bin_id'].apply(lambda x: str(x) if not pd.isna(x) else str(0))
                order_events_all5days['pbt_to_creation'] = order_events_all5days['pbt_to_creation'].apply(lambda x: float(x) if not pd.isna(x) else 0.0)
                order_events_all5days.time = pd.to_datetime(order_events_all5days.time)
                # order_events_all5days.to_csv(r'/home/ankush.j/Desktop/workname/big_data/group_df1.csv', index=False, header=True)
                order_events_all5days['PBT'] = order_events_all5days['PBT'].astype(str)
                order_events_all5days['bin_id'] = order_events_all5days['bin_id'].astype(str)
                order_events_all5days['bintags'] = order_events_all5days['bintags'].astype(str)
                order_events_all5days['breached'] = order_events_all5days['breached'].astype(str)
                order_events_all5days['breached_completed'] = order_events_all5days['breached_completed'].astype(str)
                if 'breached_order_complete' in order_events_all5days.columns:
                    order_events_all5days['breached_order_complete'] = order_events_all5days['breached_order_complete'].astype(
                        str)
                if 'breached_order_created' in order_events_all5days.columns:
                    order_events_all5days['breached_order_created'] = order_events_all5days['breached_order_created'].astype(str)

                if 'breached_order_tasked' not in order_events_all5days.columns:
                    order_events_all5days['breached_order_tasked'] = ""
                order_events_all5days['breached_order_tasked'] = order_events_all5days['breached_order_tasked'].astype(str)
                order_events_all5days['breached_tasked'] = order_events_all5days['breached_tasked'].astype(str)
                order_events_all5days['created_to_completed'] = order_events_all5days['created_to_completed'].astype(float)
                order_events_all5days['creation_to_tasked'] = order_events_all5days['creation_to_tasked'].astype(float)
                order_events_all5days['external_service_request_id'] = order_events_all5days[
                    'external_service_request_id'].astype(str)
                order_events_all5days['first_item_picked_to_order_complete'] = order_events_all5days[
                    'first_item_picked_to_order_complete'].astype(float)
                order_events_all5days['installation_id'] = order_events_all5days['installation_id'].astype(str)
                order_events_all5days['order_created_time'] = order_events_all5days['order_created_time'].astype(str)
                order_events_all5days['order_flow_name'] = order_events_all5days['order_flow_name'].astype(str)
                order_events_all5days['order_id'] = order_events_all5days['order_id'].astype(str)
                order_events_all5days['order_inventory_awaited'] = order_events_all5days['order_inventory_awaited'].astype(
                    str)
                order_events_all5days['order_recalculated_for_deadlock'] = order_events_all5days[
                    'order_recalculated_for_deadlock'].astype(str)
                order_events_all5days['order_tasked_time'] = order_events_all5days['order_tasked_time'].astype(str)
                order_events_all5days['order_temporary_unfulfillable'] = order_events_all5days[
                    'order_temporary_unfulfillable'].astype(str)
                order_events_all5days['order_unfulfillable'] = order_events_all5days['order_unfulfillable'].astype(str)
                order_events_all5days['pbt_to_completion'] = order_events_all5days['pbt_to_completion'].astype(float)
                order_events_all5days['pbt_to_creation'] = order_events_all5days['pbt_to_creation'].astype(float)
                order_events_all5days['pbt_to_task'] = order_events_all5days['pbt_to_task'].astype(float)
                order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'].astype(str)
                order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'].astype(str)
                order_events_all5days['pps_id'] = order_events_all5days['pps_id'].astype(str)
                order_events_all5days['tasked_to_first_item_picked'] = order_events_all5days[
                    'tasked_to_first_item_picked'].astype(float)
                order_events_all5days['time_to_pick'] = order_events_all5days['time_to_pick'].astype(float)
                order_events_all5days['total_picked_quantity'] = order_events_all5days['total_picked_quantity'].astype(float)
                order_events_all5days['total_time'] = order_events_all5days['total_time'].astype(float)

                order_events_all5days = order_events_all5days.set_index('time')
                if 'station_type_y' in order_events_all5days.columns:
                    order_events_all5days = order_events_all5days.drop(['station_type_y','storage_type_y'],axis=1)
                    order_events_all5days = order_events_all5days.rename(columns={'station_type_x':'station_type','storage_type_x':'storage_type'})
                
                # return order_events_all5days
                if not order_events_all5days.empty:
                    self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
                    self.write_client.writepoints(order_events_all5days, "order_lifecycle_alteryx", db_name=self.tenant_info["out_db_name"],tag_columns=['pps_id','station_type','storage_type'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
                else:
                    filter = {"site": self.tenant_info['Name'], "table": "order_lifecycle_alteryx"} 
                    new_last_run = {"last_run": self.end_date}
                    self.utilfunction.update_dag_last_run(filter,new_last_run) 

                    # self.write_client.writepoints(dag_last_run_status, "last_dag_run_status", db_name=self.tenant_info["alteryx_out_db_name"],
                #                                 tag_columns=['table'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        else:
            filter = {"site": self.tenant_info['Name'], "table": "order_lifecycle_alteryx"} 
            new_last_run = {"last_run": self.end_date}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
        return
# self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"])
# self.write_client.writepoints(order_events_all5days[['pps_id','installation_id','order_id','order_created_time','order_tasked_time','PBT','total_picked_quantity','bin_id','breached','order_temporary_unfulfillable','order_inventory_awaited','order_recalculated_for_deadlock','order_unfulfillable','creation_to_tasked','tasked_to_first_item_picked','first_item_picked_to_order_complete','time_to_pick','pbt_to_completion','pbt_to_task','created_to_completed','pbt_to_creation','total_time','breached_order_complete','breached_order_created','breached_order_tasked','breached_completed','breached_tasked','pick_before_time','pick_after_time','order_flow_name','bintags','external_service_request_id']], "order_lifecycle_alteryx", db_name=self.tenant_info["out_db_name"],tag_columns=['pps_id'])
#self.write_client.writepoints(order_events_all5days[['pps_id','installation_id','order_id','total_picked_quantity','pps_id','bin_id','creation_to_tasked','tasked_to_first_item_picked','first_item_picked_to_order_complete','time_to_pick','pbt_to_completion','pbt_to_task','created_to_completed','pbt_to_creation','total_time']], "order_lifecycle_alteryx", db_name=self.tenant_info["out_db_name"],tag_columns=['pps_id'])

#self.write_client.writepoints(order_events_all5days[['pps_id','installation_id','order_id','order_created_time','order_tasked_time','PBT','total_picked_quantity','bin_id','breached','order_temporary_unfulfillable','order_inventory_awaited','order_recalculated_for_deadlock','order_unfulfillable','creation_to_tasked','tasked_to_first_item_picked','first_item_picked_to_order_complete','time_to_pick','pbt_to_completion','pbt_to_task','created_to_completed','pbt_to_creation','total_time','breached_order_complete','breached_order_created','breached_order_tasked','breached_completed','breached_tasked','pick_before_time','pick_after_time','order_flow_name']], "order_lifecycle_alteryx", db_name=self.tenant_info["out_db_name"],tag_columns=['pps_id'])


# # -----------------------------------------------------------------------------
# ## Task definations
# ## -----------------------------------------------------------------------------
with DAG(
    'Order_LifeCycle',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of order lifecycle',
    schedule_interval = '30 * * * *',
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
            if tenant['Active'] == "Y" and tenant['Order_Lifecycle'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                order_lifecycle_final_task = PythonOperator(
                    task_id='order_lifecycle_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(OrderLifecycle().order_lifecycle_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
            # tenant = {"Name": "site", "Butler_ip": Butler_ip, "influx_ip": influx_ip, "influx_port": influx_port, \
            #           "write_influx_ip": write_influx_ip, "write_influx_port": influx_port, \
            #           "out_db_name": db_name,\
            #           "Postgres_pf_user": Postgres_pf_user, "Postgres_pf_password": Postgres_pf_password, "sslrootcert" : sslrootcert, "sslcert" : sslcert,\
            #           "sslkey" : sslkey, "Postgres_pf":Postgres_pf, "Postgres_pf_port":Postgres_pf_port,"streaming": streaming,"TimeZone":TimeZone
            # }
            tenant = CommonFunction().get_tenant_info()
            order_lifecycle_final_task = PythonOperator(
                task_id='order_lifecycle_final',
                provide_context=True,
                python_callable=functools.partial(OrderLifecycle().order_lifecycle_final,tenant_info={'tenant_info': tenant}),
                op_kwargs={
                    'tenant_info1': tenant,
                },
                execution_timeout=timedelta(seconds=3600),
            )



#
# from order_lifecycle_alteryx import OrderLifecycle
#
# import os
# Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
# influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
# write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
# influx_port = os.environ.get('INFLUX_PORT', '8086')
# db_name =os.environ.get('Out_db_name', 'airflow')
# streaming= "N"
# TimeZone ="PDT"
# srms_ip = '172.19.40.47'
# srms_port = '5435'
#
# tenant = {"Name": "site", "Butler_ip": Butler_ip, "influx_ip": influx_ip, "influx_port": influx_port, \
#           "write_influx_ip": write_influx_ip, "write_influx_port": influx_port, \
#           "out_db_name": db_name,\
#           "Postgres_pf_user": "altryx_read", "Postgres_pf_password": "YLafmbfuji$@#45!", "sslrootcert" : '', "sslcert" : '',\
#           "sslkey" : '', "Postgres_pf":srms_ip, "Postgres_pf_port":srms_port,"streaming": streaming,"TimeZone":TimeZone
# }
#
# tenant_info={'tenant_info': tenant}
# temp =OrderLifecycle()
# temp.order_lifecycle_final(tenant_info=tenant_info)
#


