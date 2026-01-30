## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
from influxdb import InfluxDBClient
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
from pandasql import sqldf
import pytz
## -----------------------------------------------------------------------------
## Influx server details
## -----------------------------------------------------------------------------
import os
Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
db_name =os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------


class IntervalThroughput:
    def get_merge_df(self, interval_start_date,interval_end_date, timeseriesdata):
        interval_start_date = interval_start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        interval_end_date = interval_end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select installation_id,pps_id,uom_quantity_int,uom_quantity,value,station_type,storage_type from item_picked where time>'{interval_start_date}' and time<='{interval_end_date}'  order by time desc"
        df_itemput = pd.DataFrame(self.read_client.query(q).get_points())
        if not df_itemput.empty:
            df_itemput['time'] = pd.to_datetime(df_itemput['time'])
            df_itemput = df_itemput.sort_values(by=['installation_id', 'pps_id', 'time'])
            df_itemput = df_itemput.reset_index()
            ttp_setup = (self.tenant_info['is_ttp_setup']=='Y')
            df_itemput = self.utilfunction.update_station_type(df_itemput,ttp_setup)
            df_itemput = self.utilfunction.update_storage_type(df_itemput,ttp_setup)            
            df_unique_pps_installation= df_itemput[['installation_id','pps_id','station_type','storage_type']]
            df_unique_pps_installation=df_unique_pps_installation.drop_duplicates()
            df_unique_pps_installation['key']= 1
            merge_df = sqldf("select a.pps_id,a.installation_id, b.ceil,b.interval_start,b.interval_end ,a.station_type,a.storage_type from df_unique_pps_installation  a inner join timeseriesdata b on a.key =b.key")
        else:
            merge_df = pd.DataFrame()
        return merge_df


    def get_item_pick(self, interval_start_date,interval_end_date, merge_df):
        interval_start_date1 = interval_start_date
        interval_start_date1 = interval_start_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
        interval_end_date = interval_end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select installation_id,user_id, pps_id,uom_quantity_int,uom_quantity,value,order_flow_name,station_type,storage_type from item_picked where time>'{interval_start_date1}' and time<='{interval_end_date}'  order by time desc"
        df_itemput = pd.DataFrame(self.read_client.query(q).get_points())
        df_itemput['time'] = pd.to_datetime(df_itemput['time'])
        df_itemput['order_flow_name']=df_itemput['order_flow_name'].fillna('default')
        df_itemput['uom_quantity_int'] = df_itemput.apply(self.utilfunction.calc_uom_quanity,axis=1)
        del df_itemput['uom_quantity']
        df_itemput = df_itemput.sort_values(by=['installation_id', 'pps_id', 'time'])
        df_itemput= df_itemput.reset_index()

        ttp_setup = (self.tenant_info['is_ttp_setup']=='Y')
        df_itemput = self.utilfunction.update_station_type(df_itemput,ttp_setup)
        df_itemput = self.utilfunction.update_storage_type(df_itemput,ttp_setup)

        df_itemput['interval_start_date']= interval_start_date
        df_itemput['ceil'] = df_itemput.apply(self.utilfunction.ceil_data,axis=1)
        df_unique__order_flow_name_pps_installation = df_itemput[['installation_id', 'pps_id','station_type','storage_type', 'ceil', 'order_flow_name']]
        df_unique__order_flow_name_pps_installation = df_unique__order_flow_name_pps_installation.drop_duplicates()
        df_unique__order_flow_name_pps_installation['OrderFlowName'] = df_unique__order_flow_name_pps_installation.groupby(['installation_id', 'pps_id','station_type','storage_type', 'ceil'])['order_flow_name'].transform(lambda x: ' '.join(x))
        df_unique__order_flow_name_pps_installation = df_unique__order_flow_name_pps_installation.drop_duplicates(
            subset=['installation_id', 'pps_id', 'station_type', 'storage_type', 'ceil', 'OrderFlowName'], keep='last')
        del df_unique__order_flow_name_pps_installation['order_flow_name']
        del df_itemput['order_flow_name']
        group_df =sqldf("select a.pps_id, a.station_type, a.storage_type, max(b.user_id) as user_id, a.installation_id,a.interval_start,a.interval_end,a.ceil, min(b.time) as Min_time,sum(b.uom_quantity_int) as sum_qty, sum(b.value) sum_value  from merge_df a left join  df_itemput b on  a.pps_id= b.pps_id and a.installation_id= b. installation_id and a.ceil= b.ceil and a.station_type= b.station_type and a.storage_type= b.storage_type group by a.pps_id, a.installation_id,a.interval_start,a.interval_end,a.ceil, a.station_type, a.storage_type")
        group_df= pd.merge(group_df,df_unique__order_flow_name_pps_installation,how='left', on =['installation_id', 'pps_id', 'ceil','station_type','storage_type'])
        group_df['flag'] = 1
        group_df['sum_qty']=group_df['sum_qty'].fillna(0)
        group_df['sum_value'] = group_df['sum_value'].fillna(0)
        return group_df

    def apply_start_date(self, x):
        starttime =""
        if x['event_name']=='msu_wait' and x['event_name_lower'] !='msu_wait':
            starttime = x['time']
        return starttime

    def apply_start_date2(self, x):
        starttime =""
        if x['event_name']=='msu_wait' and x['start_date'] =="" and x['start_date_lower']!="" \
                and x['pps_id_lower']==x['pps_id']\
                and x['installation_id_lower']==x['installation_id']:
            starttime = x['start_date_lower']
        else:
            starttime = x['start_date']
        return starttime

    def apply_start_date_new(self, df):
        for x in df.index:
            if x >0:
                start_date_lower = df['start_date'][x- 1]
                if df['event_name'][x]=='msu_wait' and df['start_date'][x] =="" and start_date_lower!="" \
                        and df['pps_id_lower'][x]==df['pps_id'][x]\
                        and df['installation_id_lower'][x]==df['installation_id'][x]:
                    df['start_date'][x] = start_date_lower
        return df

    def apply_end_date(self, x):
        endtime =""
        if x['event_name']=='msu_wait' and x['event_name_upper'] !='msu_wait' \
                and x['pps_id_upper'] == x['pps_id'] \
                and x['installation_id_upper'] == x['installation_id']:
            endtime = x['time']
        return endtime

    def get_flow_event(self, interval_start_date,interval_end_date,merge_df):
        interval_start_date1 = interval_start_date
        interval_start_date1 = interval_start_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
        interval_end_date = interval_end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select * from flow_events where time>'{interval_start_date1}'  and time<='{interval_end_date}' and process_mode ='pick' order by time"
        df2_flowevents = pd.DataFrame(self.read_client.query(q).get_points())
        if not df2_flowevents.empty:
            l = ['container_type', 'eaches_picked', 'flow_state', 'host,ppsbin_id', 'process_mode',
                 'quantity_in_container', 'seat_name', 'seat_type', 'state_name']
            for col in l:
                if col in df2_flowevents.columns:
                    del df2_flowevents[col]
            if 'user_loggedin' not in df2_flowevents:
                df2_flowevents['user_loggedin']=''
            df2_flowevents['event_name'] = df2_flowevents['event_name'].apply(
                lambda x: 'msu_wait' if x == 'waiting_for_msu' else x)
            df2_flowevents['quantity_int'] = df2_flowevents.apply(self.utilfunction.calc_flowevent_quanity, axis=1)
            df2_flowevents['quantity_int'] = df2_flowevents['quantity_int'].apply(lambda x:int(x) if not pd.isna(x) else 0)
            del df2_flowevents['quantity']
            df2_flowevents = df2_flowevents.sort_values(by=['installation_id', 'pps_id', 'time'])
            df2_flowevents= df2_flowevents.reset_index()
            ttp_setup = (self.tenant_info['is_ttp_setup']=='Y')
            df2_flowevents = self.utilfunction.update_station_type(df2_flowevents,ttp_setup)
            df2_flowevents = self.utilfunction.update_storage_type(df2_flowevents,ttp_setup)
            df2_flowevents= self.utilfunction.lower_shift_data(df2_flowevents)
            df2_flowevents= self.utilfunction.upper_shift_data(df2_flowevents)
            df2_flowevents['time'] = pd.to_datetime(df2_flowevents['time'])
            df2_flowevents['start_date'] = df2_flowevents.apply(self.apply_start_date, axis=1)
            df2_flowevents = self.apply_start_date_new(df2_flowevents)
            df2_flowevents['end_date'] = df2_flowevents.apply(self.apply_end_date, axis=1)
            #df2_flowevents.to_csv(r'/home/ankush.j/Desktop/workname/big_data/df2_flowevents.csv', index=False, header=True)
            indexdata=df2_flowevents[(df2_flowevents['start_date']=='') | (df2_flowevents['end_date']=='')].index
            df2_flowevents.drop(indexdata, inplace=True)
            df2_flowevents['interval_start_date']=interval_start_date
            if not df2_flowevents.empty:
                df2_flowevents['ceil'] = df2_flowevents.apply(self.utilfunction.ceil_data, axis=1)
            else:
                df2_flowevents['ceil'] =None
            df2_flowevents = df2_flowevents.drop(columns=['event_name_lower','pps_id_lower','installation_id_lower','event_name_upper','pps_id_upper'\
                ,'installation_id_upper','start_date','end_date' ])
            group_df = sqldf(
                "select a.pps_id, a.station_type, a.storage_type, max(b.user_loggedin) as user_id, a.installation_id,a.interval_start,a.interval_end,a.ceil,min(b.time) time from merge_df a inner join  df2_flowevents b on  a.pps_id= b.pps_id and a.installation_id= b. installation_id and a.ceil= b.ceil group by a.pps_id, a.installation_id,a.interval_start,a.interval_end, a.station_type, a.storage_type ")
        else:
            group_df = sqldf(
                "select 0 as user_id, a.station_type, a.storage_type, a.pps_id, a.installation_id,a.interval_start,a.interval_end,a.ceil from merge_df a  where 1=2 group by a.pps_id, a.installation_id,a.interval_start,a.interval_end, a.station_type, a.storage_type")
            group_df['time'] = None
        return group_df

    def get_interval_stop_date(self):
        q = f"select interval_stop from  interval_throughput where time>='{self.start_date}' order by time desc limit 1"
        try:
            df_time =pd.DataFrame(self.client.query(q).get_points())
            df_time['interval_stop'] = pd.to_datetime(df_time['interval_stop'])
            time = df_time['interval_stop'][0].to_pydatetime()
            return time
        except:
            pass
        return pd.to_datetime(self.start_date,utc=True)

    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client=InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("interval_throughput", self.tenant_info)
        check_end_date = datetime.now()
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>'{check_start_date}' and time<='{check_end_date}'  limit 1"
        df_itempick = pd.DataFrame(self.client.query(q).get_points())
        if df_itempick.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("interval_throughput", '1h', self.tenant_info)
                if daterange.empty:
                    daterange = self.client.get_datetime_interval3("interval_throughput", '15min', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e
    # sd: 2022-11-09 07:00:00
    # ed: 2022-11-09 14:00:00
    def final_call1(self, end_date, **kwargs):
        #self.tenant_info = tenant_info['tenant_info']
        self.utilfunction = CommonFunction()
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        self.start_date = self.client.get_start_date("interval_throughput", self.tenant_info)
        self.start_date =pd.to_datetime(self.start_date).strftime("%Y-%m-%d %H:%M:%S")
        self.start_date=self.get_interval_stop_date()
        self.original_start_date = pd.to_datetime(self.start_date)+timedelta(minutes=1) #adding one minute for corner case
        # q = f"select * from item_picked where time>'{self.start_date}' order by time limit 2000"
        # df = pd.DataFrame(self.client.query(q).get_points())
        # df = df.sort_values(by=['time'], ascending=[False])
        # df= df.reset_index()
        #self.end_date = df['time'][0]
        #print(self.end_date)
        # self.end_date = pd.datetime(self.end_date, utc=True)
       # self.end_date = datetime.now(timezone.utc)
        self.start_date = self.start_date - timedelta(hours=1)
        # if self.tenant_info["Name"]=="Simulation":
        #     self.start_date = self.end_date - timedelta(minutes=75)
        # print(self.start_date)
        # print(self.end_date)
        #self.end_date = datetime.now(timezone.utc)
        self.start_date =self.start_date.astimezone(pytz.utc)
        self.end_date =self.end_date.astimezone(pytz.utc)


        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
        q = f"select installation_id,pps_id,uom_quantity_int,uom_quantity,value,order_flow_name  from item_picked where time>'{self.original_start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}' and time<='{self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}'  order by time desc limit 1"
        df_itempick = pd.DataFrame(self.read_client.query(q).get_points())
        if df_itempick.empty:
            filter = {"site": self.tenant_info['Name'], "table": "interval_throughput"} 
            new_last_run = {"last_run": self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}
            self.utilfunction.update_dag_last_run(filter,new_last_run)  
            return

        # start_date=datetime(2022, 11, 9, 12, 30, 0, 119205)
        # start_date =start_date.astimezone(pytz.utc)
        # end_date=datetime(2022, 11, 9, 19, 30, 0, 119205)
        # end_date =end_date.astimezone(pytz.utc)



        init_date_frame = self.utilfunction.create_startdate_enddate_frame(self.start_date, self.end_date)
        #init_date_frame['interval_start_date']= init_date_frame['interval_start_date']- pd.Timedelta(hours = 1)
        #init_date_frame['start_date'] = init_date_frame['start_date'] - pd.Timedelta(hours=1)

        timeseriesdata = self.utilfunction.create_time_series_data(init_date_frame["interval_start_date"][0],init_date_frame["interval_end_date"][0], '3min')
        merge_df = self.get_merge_df(init_date_frame["interval_start_date"][0],init_date_frame["interval_end_date"][0], timeseriesdata)
        if not merge_df.empty:
            df_item_pick = self.get_item_pick(init_date_frame["interval_start_date"][0],init_date_frame["interval_end_date"][0], merge_df)
            df2_flowevents = self.get_flow_event(init_date_frame["interval_start_date"][0],init_date_frame["interval_end_date"][0], merge_df)
            zero_data_frame=df_item_pick[df_item_pick['sum_qty']==0]
            non_zero_data_frame=df_item_pick[df_item_pick['sum_qty']>0]

            df1 = sqldf(
                "select b.pps_id,b.station_type, b.storage_type, b.installation_id, b.interval_start, max(a.user_id) as user_id, b.interval_end,b.ceil,min(a.time) as Min_time_wfm, b.Min_time,0 as sum_qty,0 as sum_value,b.flag as flag , b.OrderFlowName from df2_flowevents a inner join zero_data_frame b on   a.pps_id= b.pps_id and a.installation_id= b. installation_id and a.ceil= b.ceil and a.station_type= b.station_type and a.storage_type= b.storage_type group by b.installation_id, b.pps_id,b.interval_start,b.interval_end, b.flag,b.ceil,b.Min_time,b.station_type, b.storage_type")
            if not df1.empty:
                df1["Min_time"] = df1[['Min_time','Min_time_wfm']].apply(lambda x: x['Min_time_wfm'] if pd.isna(x['Min_time'])   else x['Min_time'], axis=1)
            df2=sqldf("select b.pps_id,b.station_type, b.storage_type, max(b.user_id) as user_id, b.installation_id, b.interval_start,b.interval_end,b.ceil,min(b.Min_time) as Min_time_wfm, b.Min_time,0 as sum_qty,0 as sum_value,0  as flag, b.OrderFlowName from zero_data_frame b left join df2_flowevents a on   a.pps_id= b.pps_id and a.installation_id= b. installation_id and a.ceil= b.ceil and a.station_type= b.station_type and a.storage_type= b.storage_type where a.pps_id is null group by b.installation_id, b.pps_id,b.interval_start,b.interval_end,b.ceil,b.Min_time,b.station_type, b.storage_type")
            final_df= pd.concat([non_zero_data_frame, df1, df2], axis=0)
            del final_df['Min_time_wfm']

            if not final_df.empty:            
                final_df= final_df.sort_values(by=['installation_id','pps_id','ceil'])
                final_df= final_df.reset_index()   
                ttp_setup = (self.tenant_info['is_ttp_setup']=='Y')
                final_df = self.utilfunction.update_station_type(final_df,ttp_setup)
                final_df = self.utilfunction.update_storage_type(final_df,ttp_setup)
                final_df['15_end']=final_df['ceil']-4
                final_df['30_end'] = final_df['ceil'] - 9
                final_df['60_end'] = final_df['ceil'] - 19
                final_df['15_end'] = final_df['15_end'].apply(lambda x: x if x >0 else None)
                final_df['30_end'] = final_df['30_end'].apply(lambda x: x if x >0 else None)
                final_df['60_end'] = final_df['60_end'].apply(lambda x: x if x >0 else None)
                final_df["pps_id_4"]= final_df['pps_id'].shift(+4)
                final_df["installation_id_4"] = final_df['installation_id'].shift(+4)
                final_df["installation_id_4"] = final_df['installation_id'].shift(+4)
                final_df["station_type_4"] = final_df['station_type'].shift(+4)
                final_df["storage_type_4"] = final_df['storage_type'].shift(+4)
                final_df["pps_id_9"] = final_df['pps_id'].shift(+9)
                final_df["installation_id_9"] = final_df['installation_id'].shift(+9)
                final_df["station_type_9"] = final_df['station_type'].shift(+9)
                final_df["storage_type_9"] = final_df['storage_type'].shift(+9)
                final_df["pps_id_19"] = final_df['pps_id'].shift(+19)
                final_df["installation_id_19"] = final_df['installation_id'].shift(+19)
                final_df["station_type_19"] = final_df['station_type'].shift(+19)
                final_df["storage_type_19"] = final_df['storage_type'].shift(+19)
                final_df["msu_15_1"] =final_df[['pps_id_4','installation_id_4','pps_id','installation_id','ceil','15_end','station_type_4','station_type','storage_type_4','storage_type']].apply(lambda x: 1 if x["pps_id_4"] ==x["pps_id"] and x["installation_id_4"] ==x["installation_id"] and x["storage_type_4"] ==x["storage_type"] and x["storage_type_4"] ==x["storage_type"]  and x['ceil']>=x['15_end'] else 0, axis=1)
                final_df["msu_30_1"] =final_df[['pps_id_9','installation_id_9','pps_id','installation_id','ceil','30_end','station_type_9','station_type','storage_type_9','storage_type']].apply(lambda x: 1 if x["pps_id_9"] ==x["pps_id"] and x["installation_id_9"] ==x["installation_id"]  and x["storage_type_9"] ==x["storage_type"] and x["storage_type_9"] ==x["storage_type"] and x['ceil']>=x['30_end'] else 0, axis=1)
                final_df["msu_60_1"] =final_df[['pps_id_19','installation_id_19','pps_id','installation_id','ceil','60_end','station_type_19','station_type','storage_type_19','storage_type']].apply(lambda x: 1 if x["pps_id_19"] ==x["pps_id"] and x["installation_id_19"] ==x["installation_id"]  and x["storage_type_19"] ==x["storage_type"] and x["storage_type_19"] ==x["storage_type"] and x['ceil']>=x['60_end'] else 0, axis=1)
                final_df["msu_15_2"] =final_df[['pps_id_4','installation_id_4','pps_id','installation_id','ceil','15_end','station_type_4','station_type','storage_type_4','storage_type']].apply(lambda x: 1 if x["pps_id_4"] ==x["pps_id"] and x["installation_id_4"] ==x["installation_id"]  and x["storage_type_4"] ==x["storage_type"] and x["storage_type_4"] ==x["storage_type"]  else 0, axis=1)
                final_df["msu_30_2"] =final_df[['pps_id_9','installation_id_9','pps_id','installation_id','ceil','30_end','station_type_9','station_type','storage_type_9','storage_type']].apply(lambda x: 1 if x["pps_id_9"] ==x["pps_id"] and x["installation_id_9"] ==x["installation_id"]   and x["storage_type_9"] ==x["storage_type"] and x["storage_type_9"] ==x["storage_type"] else 0, axis=1)
                final_df["msu_60_2"] =final_df[['pps_id_19','installation_id_19','pps_id','installation_id','ceil','60_end','station_type_19','station_type','storage_type_19','storage_type']].apply(lambda x: 1 if x["pps_id_19"] ==x["pps_id"] and x["installation_id_19"] ==x["installation_id"]  and x["storage_type_19"] ==x["storage_type"] and x["storage_type_19"] ==x["storage_type"] else 0, axis=1)

                final_df["msu_15"]=final_df["flag"].rolling(min_periods=1, window=5).sum()
                final_df["msu_30"]= final_df["flag"].rolling(min_periods=1, window=10).sum()
                final_df["msu_60"]= final_df["flag"].rolling(min_periods=1, window=20).sum()
                final_df["msu_15"] =final_df[['msu_15','msu_15_1']].apply(lambda x: x['msu_15'] if x['msu_15_1'] >0 else 0, axis=1)
                final_df["msu_30"] =final_df[['msu_30','msu_30_1']].apply(lambda x: x['msu_30'] if x['msu_30_1'] >0 else 0, axis=1)
                final_df["msu_60"] =final_df[['msu_60','msu_60_1']].apply(lambda x: x['msu_60'] if x['msu_60_1'] >0 else 0, axis=1)
                final_df["UPH_15min_value"] =final_df["sum_value"].rolling(min_periods=1, window=5).sum()*20/final_df["msu_15"]
                final_df["UPH_30min_value"] = final_df["sum_value"].rolling(min_periods=1, window=10).sum()*20/final_df["msu_30"]
                final_df["UPH_60min_value"] = final_df["sum_value"].rolling(min_periods=1, window=20).sum()*20/final_df["msu_60"]

                final_df["UPH_15min"] =final_df["sum_qty"].rolling(min_periods=1, window=5).sum()*20/final_df["msu_15"]
                final_df["UPH_30min"] = final_df["sum_qty"].rolling(min_periods=1, window=10).sum()*20/final_df["msu_30"]
                final_df["UPH_60min"] = final_df["sum_qty"].rolling(min_periods=1, window=20).sum()*20/final_df["msu_60"]

                final_df["UPH_15min_value"] =final_df[['UPH_15min_value','msu_15_2']].apply(lambda x: (x['UPH_15min_value']) if x['msu_15_2'] >0 else 0, axis=1)
                final_df["UPH_30min_value"] =final_df[['UPH_30min_value','msu_30_2']].apply(lambda x: (x['UPH_30min_value']) if x['msu_30_2'] >0 else 0, axis=1)
                final_df["UPH_60min_value"] =final_df[['UPH_60min_value','msu_60_2']].apply(lambda x: (x['UPH_60min_value']) if x['msu_60_2'] >0 else 0, axis=1)
                final_df["UPH_15min"] =final_df[['UPH_15min','msu_15_2']].apply(lambda x: (x['UPH_15min']) if x['msu_15_2'] >0 else 0, axis=1)
                final_df["UPH_30min"] =final_df[['UPH_30min','msu_30_2']].apply(lambda x: (x['UPH_30min']) if x['msu_30_2'] >0 else 0, axis=1)
                final_df["UPH_60min"] =final_df[['UPH_60min','msu_60_2']].apply(lambda x: (x['UPH_60min']) if x['msu_60_2'] >0 else 0, axis=1)
                if final_df.empty:
                    return
                final_df = final_df[(final_df.flag !=0)]
                if final_df.empty:
                    return
                self.start_date = self.start_date + timedelta(hours=1)
                final_df["start"]=self.start_date
                final_df['interval_start']= pd.to_datetime(final_df['interval_start'])
                final_df["flag2"] = final_df.apply(self.utilfunction.date_compare, axis=1)
                final_df = final_df[(final_df["flag2"])]


                if not final_df.empty:
                    final_df= final_df.drop(['flag2','15_end','30_end', '60_end', 'pps_id_4', 'installation_id_4', 'pps_id_9',\
                        'installation_id_9', 'pps_id_19', 'installation_id_19', 'msu_15_1','msu_30_1', 'msu_60_1','msu_15', 'msu_30', 'msu_60','start', 'ceil','msu_15_2','msu_30_2','msu_60_2','station_type_4','station_type_9','station_type_19','storage_type_4','storage_type_9','storage_type_19']     , axis=1)
                    lamdatest= lambda x: "Y" if x >0 else "N"
                    final_df.flag = final_df.flag.apply(lamdatest)
                    lamdatest= lambda x: "N" if pd.isna(x['sum_qty']) or x['sum_qty']==0 else x['flag']
                    final_df.flag = final_df.apply(lamdatest, axis=1)
                    final_df.rename(columns = {'sum_qty':'Sum_uom', 'sum_value':'Sum_value',
                                                  'interval_end':'interval_stop','Min_time':'time','flag':'msu_wait_flag'}, inplace = True)
                    final_df['interval_start'] = final_df['interval_start'].astype(str)
                    final_df['interval_stop'] = final_df['interval_stop'].astype(str)
                    final_df['UPH_15min_value'] = final_df['UPH_15min_value'].astype(float)
                    final_df['UPH_30min_value'] = final_df['UPH_30min_value'].astype(float)
                    final_df['UPH_60min_value'] = final_df['UPH_60min_value'].astype(float)
                    final_df['UPH_15min'] = final_df['UPH_15min'].astype(float)
                    final_df['UPH_30min'] = final_df['UPH_30min'].astype(float)
                    final_df['UPH_60min'] = final_df['UPH_60min'].astype(float)
                    final_df['Sum_uom'] = final_df['Sum_uom'].astype(float)
                    final_df['Sum_value'] = final_df['Sum_value'].astype(float)
                    final_df['pps_id'] = final_df['pps_id'].astype('str')
                    final_df['installation_id'] = final_df['installation_id'].astype('str')
                    final_df['msu_wait_flag'] = final_df['msu_wait_flag'].astype('str')
                    final_df['user_id'] = final_df['user_id'].astype('str')
                    if 'index' in final_df.columns:
                        del final_df['index']
                    final_df.time = pd.to_datetime(final_df.time)
                    final_df = final_df.set_index('time')
                    self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                         port=self.tenant_info["write_influx_port"])
                    self.write_client.writepoints(final_df, "interval_throughput", db_name=self.tenant_info["out_db_name"],
                                                  tag_columns=['pps_id','station_type','storage_type'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        return None


with DAG(
    'Interval_throughput',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'POC for Airflow as metrics computation pipeline orchestration tool',
    schedule_interval = '*/15 * * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False,
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    import csv
    import os
    import functools
    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['Interval_throughput'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                final_task = PythonOperator(
                    task_id='Interval_throughput_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(IntervalThroughput().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='Interval_throughput',
            provide_context=True,
            python_callable=functools.partial(IntervalThroughput().final_call,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

