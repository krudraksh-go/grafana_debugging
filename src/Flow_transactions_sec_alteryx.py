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
import numpy as np

from utils.CommonFunction import InfluxData,Write_InfluxData, CommonFunction
from pandasql import sqldf
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------
import warnings

warnings.filterwarnings("ignore")

import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')

#
# dag = DAG(
#     'Flow_transactions',
#     default_args = default_args,
#     description = 'calculation of Flow_transactions GM-44036',
#     schedule_interval = timedelta(hours=1),
#     max_active_runs = 1,
#     max_active_tasks = 16,
#     concurrency = 16,
#     catchup = False
# )

## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------
class Flow_transactions:

    def apply_start_time_logic(self, df):
        for x in df.index:
            if x >= 0:
                if df['event_name'][x] == 'pick_front_no_free_bin' or (
                        df['event_name'][x] == "empty" and df['state_name'][x] == "waiting_for_free_bin") \
                        and df['event_name_lower'][x] == 'first_item_pick':
                    df['start_time'][x] = df['time'][x]
                elif df['event_name'][x] == 'first_item_pick' and df['event_name_upper'][x] != 'first_item_pick' \
                        and (df['event_name_upper'][x] != 'pick_front_no_free_bin' or \
                             (df['event_name_upper'][x] != 'empty' and df['state_name_upper'][
                                 x] != "waiting_for_free_bin")):
                    df['start_time'][x] = df['time'][x]
                elif df['event_name'][x] == 'first_item_pick' and df['event_name_upper'][x] in (
                'system_idle', 'empty', 'msu_wait'):
                    df['start_time'][x] = df['time'][x]
                else:
                    df['start_time'][x] = None
        return df

    def apply_new_column(self, df):
        GroupCol = 0
        for x in df.index:
            if df['Column2'][x] == 1:
                GroupCol = GroupCol + 1
                df['GroupCol'][x] = GroupCol
            if df['Column2'][x] == 0:
                if x == 0:
                    df['Column2'][x] = 1
                    df['GroupCol'][x] = GroupCol
                else:
                    df['Column2'][x] = df['Column2'][x - 1] + 1
                    if df["pps_id"][x] == df["pps_id"][x - 1] and df["installation_id"][x] == df["installation_id"][
                        x - 1]:
                        df['GroupCol'][x] = df['GroupCol'][x - 1]
                    else:
                        df['GroupCol'][x] = None
        return df

    def apply_case_logic(self, df):
        for x in df.index - 1:
            if x >= 0:
                if df['GroupCol'][x] == df['GroupCol'][x + 1] and df['event_name'][x] == 'first_item_pick' and \
                        df['event_name'][x + 1] == 'empty':
                    df['CaseCol'][x] = "Yes"
        return df

    def apply_msu_wait_logic(self, df):
        flag = False
        for x in df.index:
            if x >= 0:
                if not pd.isna(df['msu_wait'][x]):
                    k = x
                    only10limit=10
                    while k > 0 and only10limit >0 and df['GroupCol'][x] == df['GroupCol'][k - 1] and not flag:
                        if df['event_name'][k - 1] in ('empty', 'more_items'):
                            df['msu_wait'][x] = df['msu_wait'][x]
                            flag = True
                            break
                        k = k - 1
                        only10limit=only10limit-1
                    if not flag:
                        df['msu_wait'][x] = None
                    else:
                        flag = False
        return df


    def apply_empty_logic(self, df):
        for x in df.index - 1:
            if x >= 0:
                if df['GroupCol'][x] == df['GroupCol'][x + 1] and df['pps_id'][x] == df['pps_id'][x + 1] and \
                        df['installation_id'][x] == df['installation_id'][x + 1] and (
                        not pd.isna(df['empty'][x]) and not pd.isna(df['msu_wait'][x + 1]) and df['empty'][x] >
                        df['msu_wait'][x + 1]):
                    df['empty'][x] = df['first_empty'][x]
        return df

    def apply_msu_wait_logic1(self, df):
        for x in df.index:
            if x > 0:
                if df['GroupCol'][x] == df['GroupCol'][x - 1] and df['pps_id'][x] == df['pps_id'][x - 1] and \
                        df['installation_id'][x] == df['installation_id'][x - 1] and (
                        not pd.isna(df['msu_wait'][x]) and not pd.isna(df['empty'][x - 1]) and df['msu_wait'][x] <
                        df['empty'][x - 1]):
                    df['msu_wait'][x] = df['Last_msu_wait'][x]
        return df

    def apply_msu_wait_logic_crossgroup(self, df):
        for x in df.index:
            if x >= 0:
                if not pd.isna(df["start_time"][x]) and pd.isna(df["msu_wait"][x]):
                    flag = False
                    k = x
                    while not flag and k <= x + 2 and k + 2 <= df.index[-1]:
                        if df['GroupCol'][k + 1] == df['GroupCol'][k + 2] and df['pps_id'][k + 1] == df['pps_id'][
                            k + 2] and \
                                df['installation_id'][k + 1] == df['installation_id'][k + 2] and not pd.isna(
                            df['msu_wait'][k + 2]):
                            flag = True
                            df["msu_wait"][x] = df["msu_wait"][k + 2]
                        k = k + 1
        return df

    def apply_system_idle_logic_crossgroup(self, df):
        for x in df.index:
            if x >= 0:
                if not pd.isna(df["start_time"][x]) and pd.isna(df["system_idle"][x]):
                    flag = False
                    preflag = True
                    k = x
                    while not flag and preflag and k <= x + 3 and k + 2 <= df.index[-1]:
                        preflag = False
                        if df['GroupCol'][k + 1] == df['GroupCol'][k + 2] and df['pps_id'][k + 1] == df['pps_id'][
                            k + 2] and \
                                df['installation_id'][k + 1] == df['installation_id'][k + 2] and not pd.isna(
                            df['system_idle'][k + 2]):
                            flag = True
                            df["system_idle"][x] = df["system_idle"][k + 2]
                        elif df['GroupCol'][k + 1] == df['GroupCol'][k + 2] and df['pps_id'][k + 1] == df['pps_id'][
                            k + 2] and \
                                df['installation_id'][k + 1] == df['installation_id'][k + 2]:
                            preflag = True
                        k = k + 1
        return df

    def apply_more_items_logic_crossgroup(self, df):
        for x in df.index:
            if x >= 0:
                if not pd.isna(df["start_time"][x]) and (pd.isna(df["more_items"][x]) or df["more_items"][x]==''):
                    flag = False
                    k = x
                    while not flag and k <= x + 1 and k + 1 <= df.index[-1]:
                        if df['GroupCol'][k + 1] == df['GroupCol'][k] and df['pps_id'][k + 1] == df['pps_id'][k] and \
                                df['installation_id'][k + 1] == df['installation_id'][k] and not pd.isna(
                            df['more_items'][k + 1]):
                            flag = True
                            df["more_items"][x] = df["more_items"][k + 1]
                        k = k + 1
        return df

    def apply_empty_logic_crossgroup(self, df):
        for x in df.index:
            if x >= 0:
                if not pd.isna(df["start_time"][x]) and (pd.isna(df["empty"][x]) or df["empty"][x]==''):
                    flag = False
                    k = x
                    while not flag and k <= x + 2 and k + 1 <= df.index[-1]:
                        if df['GroupCol'][k + 1] == df['GroupCol'][k] and df['pps_id'][k + 1] == df['pps_id'][k] and \
                                df['installation_id'][k + 1] == df['installation_id'][k] and not pd.isna(
                            df['empty'][k + 1]):
                            flag = True
                            df["empty"][x] = df["empty"][k + 1]
                        k = k + 1
        return df

    def apply_first_item_pick_crossgroup(self, df):
        for x in df.index - 1:
            if x >= 0:
                if not pd.isna(df["start_time"][x]) and (pd.isna(df["first_item_pick"][x]) or pd.isna(df["first_item_pick"][x])==''):
                    if df['GroupCol'][x + 1] == df['GroupCol'][x] and df['pps_id'][x + 1] == df['pps_id'][x] and \
                            df['installation_id'][x + 1] == df['installation_id'][x] and not pd.isna(
                        df['first_item_pick'][x + 1]):
                        df["first_item_pick"][x] = df["first_item_pick"][x + 1]
        return df

    def apply_first_item_pick_next_logic(self, df):
        for x in df.index - 1:
            if x >= 0:
                if df['pps_id'][x] == df['pps_id'][x + 1] and df['installation_id'][x] == df['installation_id'][x + 1]:
                    df['first_item_pick_next'][x] = df['start_time'][x + 1]
        return df

    def apply_transaction_complete_time_logic(self, df):
        for x in df.index:
            if x >= 0:
                if not pd.isna(df['transaction_complete_time'][x]) and not pd.isna(df['msu_wait'][x]) and \
                        df['transaction_complete_time'][x] > df['msu_wait'][x]:
                    df['transaction_complete_time'][x] = df['msu_wait'][x]
                elif pd.isna(df['transaction_complete_time'][x]) and not pd.isna(df['msu_wait'][x]):
                    df['transaction_complete_time'][x] = df['msu_wait'][x]
                if pd.isna(df['transaction_complete_time'][x]):
                    df['transaction_complete_time'][x] = df['first_item_pick_next'][x]
                elif df['transaction_complete_time'][x] < df['empty'][x]:
                    df['transaction_complete_time'][x] = df['first_item_pick_next'][x]
                if pd.isna(df['transaction_complete_time'][x]):
                    df['transaction_complete_time'][x] = df['empty'][x]
                if pd.isna(df['transaction_complete_time'][x]):
                    df['transaction_complete_time'][x] = df['more_items'][x]
        return df

    def apply_damaged_item_logic(self, df):
        for x in df.index - 1:
            if x >= 0:
                if df['state_name'][x] == "waiting_for_pptl_event" and df['state_name'][
                    x + 1] == "waiting_for_exception_response_from_ui":
                    df['Damaged_item'][x] = df['Damaged_item'][x + 1]
        return df

    def Flow_transactions(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        #self.client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["write_influx_port"],
        #                         db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("flow_transactions_sec_alteryx", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df_itempick = pd.DataFrame(self.client.query(q).get_points())
        if df_itempick.empty:
            self.end_date = datetime.now(timezone.utc)
            self.Flow_transactions1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("flow_transactions_sec_alteryx", '1h', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.Flow_transactions1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def Flow_transactions1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("flow_transactions_sec_alteryx", self.tenant_info)
        self.original_start_date = pd.to_datetime(self.start_date)+timedelta(minutes=5)
        self.start_date = pd.to_datetime(self.start_date)- timedelta(minutes=30)
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0,microsecond=0)
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        utilfunction = CommonFunction()
        print(self.start_date, self.end_date, self.original_start_date)
        

        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
        q = f"select installation_id,pps_id,uom_quantity_int,uom_quantity,value,order_flow_name from item_picked where time>'{self.original_start_date}' and time<='{self.end_date}'  order by time desc limit 1"
        df_itempick = pd.DataFrame(self.read_client.query(q).get_points())
        if df_itempick.empty:
            filter = {"site": self.tenant_info['Name'], "table": "flow_transactions_sec_alteryx"} 
            new_last_run = {"last_run": self.end_date}
            utilfunction.update_dag_last_run(filter,new_last_run)
            return

        q = f"select * from flow_events where  process_mode ='pick' and seat_type ='front' and time >= '{self.start_date}' and time <= '{self.end_date}' and event_name!='login' and event_name !='logout' order by time desc"
        df_main = pd.DataFrame(self.read_client.query(q).get_points())
        # df_main['time'] = pd.to_datetime(df_main['time'])
        # df_main['time'] = df_main['time'].apply(lambda x: x.to_pydatetime())
        if not df_main.empty:
            if 'user_loggedin' not in df_main.columns:
                df_main['user_loggedin']="default"
            df_main['event_name'] = df_main['event_name'].apply(lambda x: 'msu_wait' if x == 'waiting_for_msu' else x)
            df_main["_time"] = df_main.time
            df_main.time = df_main.time.apply(lambda x: x[:26]+x[-1] if len(x) > 27 else x)
            df_main.time = df_main.time.apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ') if len(x)>20 else datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
            df_main.time = df_main.time.apply(lambda x: x.replace(microsecond=0))
            df_main['quantity'] = df_main['quantity'].astype(int)
            # df_main['pps_id'] = df_main['pps_id'].astype(int)
            df_main['quantity_in_container'] = df_main['quantity_in_container'].apply(
                lambda x: 0 if x == 'undefined' else x)
            df_main = utilfunction.create_event_name_unique(df_main)
            if df_main.empty:
                return
            df_main = df_main.sort_values(by=['installation_id', 'pps_id', '_time'], ascending=[False, False, False])
            df_main = utilfunction.reset_index(df_main)

            ttp_setup = (self.tenant_info['is_ttp_setup']=='Y')
            df_main = utilfunction.update_station_type(df_main,ttp_setup)
            df_main = utilfunction.update_storage_type(df_main,ttp_setup)     
                   
            df_main = utilfunction.lower_shift_data_list(df_main, ['event_name'])
            df_main = utilfunction.upper_shift_data_list(df_main, ['event_name', 'state_name'])
            df_main['start_time'] = None

            df_main = self.apply_start_time_logic(df_main)
            df_main["start_time"] = df_main["start_time"].apply(lambda x: None if pd.isna(x) else x)
            df_main = df_main.drop(columns=['event_name_lower', 'event_name_upper', 'state_name_upper'])
            df_main['event_name_unique'] = \
                df_main[['event_name', 'event_name_unique', 'start_time', 'state_name']].apply(
                    lambda x: 1 if not pd.isna("start_time") and (x['event_name'] == 'pick_front_no_free_bin' or (
                                x['event_name'] == 'empty' and x['state_name'] == 'waiting_for_free_bin')) else x[
                        'event_name_unique'], axis=1)
            df_main['flag'] = df_main['event_name_unique'].apply(lambda x: 1 if x == 1 else 0)
            df_main["check_starttime"] = df_main["start_time"].apply(lambda x: False if pd.isna(x) else True)
            df1 = df_main[df_main["check_starttime"]]
            df2 = df_main[df_main["check_starttime"] == False]
            del df1["check_starttime"]
            del df2["check_starttime"]
            df1 = df1.sort_values(by=['installation_id', 'pps_id', '_time'], ascending=[False, False, True])
            df1 = utilfunction.reset_index(df1)
            df1 = utilfunction.lower_shift_data_list(df1, ['flag'])
            if not df1.empty:
                df1['start_time'] = df1[['start_time', 'flag_lower']].apply(
                    lambda x: None if x['flag_lower'] == 1 else x['start_time'], axis=1)
                df1['start_time'] = df1['start_time'].apply(lambda x: None if x == 0 else x)
                del df1['flag_lower']
            df_main = utilfunction.merge_dataframes(df1, df2)
            df_main = df_main.sort_values(by=['installation_id', 'pps_id', '_time'], ascending=[False, False, True])
            df_main = utilfunction.reset_index(df_main)
            df_main["Column2"] = df_main['start_time'].apply(lambda x: 1 if not pd.isna(x) else 0)
            df_main["GroupCol"] = df_main["Column2"]
            df_main = self.apply_new_column(df_main)
            df_main["GroupCol"] = df_main['GroupCol'].apply(lambda x: None if x == 0 else x)
            exp_pick_front_no_free_bin = lambda x: x['time'] if not pd.isna(x['start_time']) and (
                        x['event_name'] == 'pick_front_no_free_bin' \
                        or x['event_name'] == 'empty' and x['state_name'] == 'waiting_for_free_bin') else None
            df_main["pick_front_no_free_bin"] = df_main[['time', 'start_time', 'event_name', 'state_name']].apply(
                exp_pick_front_no_free_bin, axis=1)

            exp_first_item_pick = lambda x: x['time'] if x['event_name'] == 'first_item_pick' else None
            df_main["first_item_pick"] = df_main[['time', 'event_name']].apply(exp_first_item_pick, axis=1)
            # df_main["first_item_pick"]=pd.to_datetime(df_main["first_item_pick"])
            exp_empty = lambda x: x['time'] if x['event_name'] == 'empty' and pd.isna(x['start_time']) else None
            df_main["empty"] = df_main[['time', 'event_name', 'start_time']].apply(exp_empty, axis=1)
            # df_main["empty"]=pd.to_datetime(df_main["empty"])
            exp_msu_wait = lambda x: x['time'] if x['event_name'] == 'msu_wait' else None
            df_main["msu_wait"] = df_main[['time', 'event_name']].apply(exp_msu_wait, axis=1)
            # df_main["msu_wait"]=pd.to_datetime(df_main["msu_wait"])
            exp_more_items = lambda x: x['time'] if x['event_name'] == 'more_items' else None
            df_main["more_items"] = df_main[['time', 'event_name']].apply(exp_more_items, axis=1)
            # df_main["more_items"]=pd.to_datetime(df_main["more_items"])
            exp_system_idle = lambda x: x['time'] if x['event_name'] == 'system_idle' else None
            df_main["system_idle"] = df_main[['time', 'event_name']].apply(exp_system_idle, axis=1)
            # df_main["system_idle"]=pd.to_datetime(df_main["system_idle"])
            df_main["GroupCol"] = df_main['GroupCol'].apply(lambda x: None if x == 0 else x)
            group_df = sqldf(
                "select GroupCol,count(distinct Column2) as dColumn2, count(distinct event_name_unique) as devent_name_unique from df_main where GroupCol=GroupCol group by GroupCol")
            df_main = pd.merge(df_main, group_df, how='outer', on='GroupCol')
            # stage--1
            # --------------------------------------------------------------------------
            df_lessthan2event = df_main[(df_main["devent_name_unique"] <= 2) | (df_main["devent_name_unique"].isnull())]
            df_morethan2event = df_main[(df_main["devent_name_unique"] > 2)]
            df_lessthan2event['CaseCol'] = "No"
            df_lessthan2event = utilfunction.reset_index(df_lessthan2event)
            df_lessthan2event = self.apply_case_logic(df_lessthan2event)
            # stage-2
            distinct_group = sqldf("select distinct GroupCol as GroupCol from df_lessthan2event where CaseCol ='Yes'")
            del df_lessthan2event['CaseCol']
            df_lessthan2event = pd.merge(df_lessthan2event, distinct_group, how='inner', on='GroupCol')
            # stage-3
            # df_lessthan2event= sqldf("select a.* from df_lessthan2event as a inner join distinct_group b on a.GroupCol= b.GroupCol")
            df_main = utilfunction.merge_dataframes(df_morethan2event, df_lessthan2event)
            # stage-4
            df_main = utilfunction.reset_index(df_main)
            df_stage = self.apply_msu_wait_logic(df_main)
            df_stage['RecordID'] = np.arange(1, (df_stage.shape[0] + 1), 1)
            group_df = sqldf(
                "select GroupCol,installation_id,pps_id,flag,event_name,event_name_unique,station_type, storage_type,min(RecordID) as RecordID,min(start_time) as start_time," \
                " min(pick_front_no_free_bin) as pick_front_no_free_bin,min(first_item_pick) as first_item_pick" \
                ", min(more_items) as more_items, max(empty) empty, min(empty) first_empty, max(msu_wait) last_msu_wait,  min(msu_wait) as msu_wait , min(system_idle) as system_idle, max(system_idle) last_system_idle"
                " from df_stage group by GroupCol,installation_id,pps_id,flag,event_name_unique,event_name,station_type,storage_type order by RecordID")
            #group_df.time = group_df.time.apply(lambda x: x.replace(microsecond=0))
            del df_stage['RecordID']
            del group_df['RecordID']
            group_df = utilfunction.reset_index(group_df)
            group_df = self.apply_empty_logic(group_df)
            group_df = self.apply_msu_wait_logic1(group_df)
            group_df = group_df.sort_values(by=['installation_id', 'pps_id', 'GroupCol', 'event_name_unique'],
                                            ascending=[True, True, True, True])
            group_df = utilfunction.reset_index(group_df)
            # group_df["check"]= group_df["start_time"].apply(lambda x: True if x is not None else False)
            # group_df= group_df[group_df["check"]]
            # del group_df["check"]
            # group_df=utilfunction.reset_index(group_df)
            # group_df["first_item_pick"]=group_df["b_first_item_pick"]
            group_df = self.apply_first_item_pick_crossgroup(group_df)
            group_df = self.apply_more_items_logic_crossgroup(group_df)
            group_df = self.apply_empty_logic_crossgroup(group_df)
            group_df = self.apply_msu_wait_logic_crossgroup(group_df)
            group_df = self.apply_system_idle_logic_crossgroup(group_df)


            # stage-5
            # new_group_df=sqldf("select  GroupCol,installation_id,pps_id,min(first_item_pick) as first_item_pick,min(more_items) as more_items,min(empty) as empty,min(msu_wait) as msu_wait, min(system_idle) as system_idle from group_df group by GroupCol,installation_id,pps_id")
            # query ="select a.*,b.first_item_pick as b_first_item_pick,b.more_items as b_more_items, b.empty as b_empty, b.msu_wait as b_msu_wait, b.system_idle as b_system_idle " \
            #        "from group_df a left join new_group_df b on a.installation_id= b.installation_id and a.pps_id= b.pps_id and a.GroupCol= b.GroupCol"
            #
            # group_df = sqldf(query)
            # group_df["first_item_pick"]=group_df["b_first_item_pick"]
            # group_df["more_items"]=group_df["b_more_items"]
            # group_df["empty"]=group_df["b_empty"]
            # group_df["msu_wait"]=group_df["b_msu_wait"]
            # group_df["system_idle"]=group_df["b_system_idle"]
            #
            # group_df=group_df.drop(columns=['b_first_item_pick','b_more_items','b_empty','b_msu_wait','b_system_idle'])
            group_df["check"] = group_df["start_time"].apply(lambda x: True if not pd.isna(x) else False)
            if group_df.empty:
                filter = {"site": self.tenant_info['Name'], "table": "flow_transactions_sec_alteryx"} 
                new_last_run = {"last_run": self.end_date}
                utilfunction.update_dag_last_run(filter,new_last_run)
                return
            group_df = group_df[group_df["check"]]
            del group_df["check"]
            group_df = utilfunction.reset_index(group_df)

            group_df = group_df.sort_values(by=['installation_id', 'pps_id', 'GroupCol'], ascending=[True, True, True])
            group_df = utilfunction.reset_index(group_df)
            group_df["first_item_pick_next"] = None
            group_df = self.apply_first_item_pick_next_logic(group_df)
            group_df['empty'] = group_df['empty'].fillna('')
            group_df['PBA'] = None
            group_df = utilfunction.datediff_in_sec(group_df, 'first_item_pick', 'start_time', 'PBA')
            group_df['PBA'] = group_df[['PBA', 'flag']].apply(lambda x: x['PBA'] if x['flag'] == 1 else None, axis=1)
            group_df['start_to_first_pick_time'] = None
            group_df['first_pick_to_last_pick_time'] = None
            group_df = utilfunction.datediff_in_sec(group_df, 'more_items', 'first_item_pick', 'start_to_first_pick_time')
            group_df = utilfunction.datediff_in_sec(group_df, 'empty', 'first_item_pick', 'start_to_first_pick_time')
            group_df = utilfunction.datediff_in_sec(group_df, 'empty', 'more_items', 'first_pick_to_last_pick_time')
            group_df["transaction_complete_time"] = group_df["system_idle"]
            group_df = self.apply_transaction_complete_time_logic(group_df)
            group_df['last_pick_to_end_time'] = None
            group_df = utilfunction.datediff_in_sec(group_df, 'transaction_complete_time', 'empty', 'last_pick_to_end_time')
            group_df.start_time = group_df.start_time.apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))
            group_df.start_time = group_df.start_time.apply(lambda x: x.replace(microsecond=0))

            group_df['qty_key'] = group_df[['start_time', 'event_name', 'GroupCol']].apply(
                lambda x: str(x['start_time']) + str(x['event_name']) + str(
                    x['GroupCol'] if not pd.isna(x['start_time']) else None), axis=1)
            df_stage['qty_key'] = df_stage[['start_time', 'event_name', 'GroupCol']].apply(
                lambda x: str(x['start_time']) + str(x['event_name']) + str(
                    x['GroupCol'] if not pd.isna(x['start_time']) else None), axis=1)
            df_stage['Damaged_item'] = df_stage['event_name'].apply(
                lambda x: "Damaged" if x == "pick_front_missing_or_unscannable_damaged_item" else "False")
            df_stage = self.apply_damaged_item_logic(df_stage)
            df1 = df_stage
            df1['transaction_complete_time'] = df1['time']
            df2 = sqldf(
                "select GroupCol,installation_id,pps_id,ppsbin_id, max(quantity) as quantity from df_stage where ppsbin_id <> 'undefined' group by GroupCol,installation_id,pps_id,ppsbin_id")
            df2 = sqldf(
                "select GroupCol,installation_id,pps_id,max(ppsbin_id) as last_ppsbin_id, sum(quantity) as quantity from df2 group by GroupCol,installation_id,pps_id")
            df3 = sqldf(
                'select a.* , b.quantity as b_quanity, b.last_ppsbin_id from df_stage a inner join df2 as b on a.GroupCol=b.GroupCol')
            df3["quantity"] = df3["b_quanity"]
            df3["ppsbin_id"] = df3["last_ppsbin_id"]
            df3 = df3.drop(columns=['b_quanity', 'last_ppsbin_id'])
            # df1=sqldf('update a set ppsbin_id = df2.ppsbin_id, quanity= df2.quantity from df_stage as  a inner join df2 on a.GroupCol=df2.GroupCol')
            #group_df.to_csv(r'/home/ankush.j/Desktop/workname/big_data/group_df1.csv', index=False, header=True)
            #df3.to_csv(r'/home/ankush.j/Desktop/workname/big_data/df3.csv', index=False, header=True)
            group_df = sqldf(
                'select a.* , b.user_loggedin user_loggedin, b.quantity_in_container quantity_in_container, b.container_type,b._time as time, b.damaged_item as damaged_item, b.quantity as quantity, b.ppsbin_id as ppsbin_id,b.installation_id from group_df a inner join df3 as b on a.qty_key=b.qty_key')
            df_check = df1.groupby(['transaction_complete_time'], as_index=False).agg(time2=('time', 'max'), )
            group_df = sqldf(
                "select a.*,b.time2 as time2 from group_df a left join df_check b on a.transaction_complete_time= b.transaction_complete_time")
            group_df['first_pick_to_last_pick_time'] = group_df['first_pick_to_last_pick_time'].fillna(0)
            group_df['PBA'] = group_df['PBA'].fillna(0)
            group_df['total_time'] = group_df['PBA'] + group_df['start_to_first_pick_time'] + group_df[
                'first_pick_to_last_pick_time'] + group_df['last_pick_to_end_time']
            #group_df['per_item_time'] = group_df['total_time'] / group_df['quantity']
            group_df['per_item_time'] = group_df[['total_time', 'quantity']].apply(
                lambda x: x['total_time'] / x['quantity'] if x['quantity'] > 0 else 0, axis=1)
            group_df['per_item_time'] = group_df['per_item_time'].fillna(0)
            group_df["check_flag"] = group_df.time2.apply(lambda x: not pd.isna(x))
            group_df = group_df[(group_df["check_flag"])]
            group_df.drop(['level_0', 'index', 'GroupCol','flag','event_name','event_name_unique',
            'start_time','pick_front_no_free_bin','first_item_pick','more_items','empty','first_empty',
            'last_msu_wait','msu_wait','system_idle','last_system_idle','first_item_pick_next','transaction_complete_time'\
            ,'qty_key','time2','check_flag'], axis=1, inplace=True)
            # take some action on group_df and df
            group_df= group_df.fillna(0)
            group_df = group_df.rename(columns={'user_loggedin': 'user_id'})
            group_df.time = pd.to_datetime(group_df.time)
            # group_df['pps_id'] = group_df['pps_id'].astype('string')
            # group_df['ppsbin_id'] = group_df['ppsbin_id'].astype('string')
            # group_df['container_type'] = group_df['container_type'].astype('string')
            # group_df['damaged_item'] = group_df['damaged_item'].astype('string')
            # group_df['installation_id'] = group_df['installation_id'].astype('string')
            # group_df['user_id'] = group_df['user_id'].astype('string')
            group_df['PBA'] = group_df['PBA'].astype(float)
            group_df['start_to_first_pick_time'] = group_df['start_to_first_pick_time'].astype(float)
            group_df['first_pick_to_last_pick_time'] = group_df['first_pick_to_last_pick_time'].astype(float)
            group_df['last_pick_to_end_time'] = group_df['last_pick_to_end_time'].astype(float)
            group_df['total_time'] = group_df['total_time'].astype(float)
            group_df['per_item_time'] = group_df['per_item_time'].astype(float)
            group_df['quantity'] = group_df['quantity'].astype(float)
            group_df['quantity_in_container'] = group_df['quantity_in_container'].astype(float)

            group_df = group_df.set_index('time')

            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(group_df, "flow_transactions_sec_alteryx", db_name=self.tenant_info["out_db_name"],
                                          tag_columns=['pps_id','user_id','station_type','storage_type'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            # self.write_client.writepoints(dag_last_run_status, "last_dag_run_status", db_name=self.tenant_info["alteryx_out_db_name"],
            #                         tag_columns=['table'],dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])


        return None

# -----------------------------------------------------------------------------
## Task definations
## -----------------------------------------------------------------------------
with DAG(
    'Flow_transactions',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of Orderline transactions',
    schedule_interval = '25 * * * *',
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
            if tenant['Active'] == "Y" and tenant['Flow_transactions'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                Flow_transactions_final_task = PythonOperator(
                    task_id='Flow_transactions_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(Flow_transactions().Flow_transactions,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        Flow_transactions_final_task = PythonOperator(
            task_id='Flow_transactions_final',
            provide_context=True,
            python_callable=functools.partial(Flow_transactions().Flow_transactions,tenant_info={'tenant_info': tenant}),
            op_kwargs={
                'tenant_info1': tenant,
            },
            execution_timeout=timedelta(seconds=3600),
        )

