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


class custom_IntervalThroughput_msio:
    def get_merge_df(self, interval_start_date,interval_end_date, timeseriesdata,flow_type):
        interval_start_date = interval_start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        interval_end_date = interval_end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select installation_id,pps_id,uom_quantity_int,uom_quantity,value,order_flow_name,uom_requested,order_id  from item_picked where time>'{interval_start_date}' and time<='{interval_end_date}'  order by time desc"
        df_itemput = pd.DataFrame(self.read_client.query(q).get_points())
        df_itemput = self.utilfunction.flow_event(df=df_itemput,site_name='msio')
        if not df_itemput.empty:
            df_itemput = df_itemput[df_itemput['order_flow'] == flow_type]
            df_itemput['time'] = pd.to_datetime(df_itemput['time'])
            df_itemput = df_itemput.sort_values(by=['installation_id', 'pps_id', 'time'])
            df_itemput=df_itemput.reset_index()
            df_unique_pps_installation= df_itemput[['installation_id','pps_id','order_flow']]
            df_unique_pps_installation=df_unique_pps_installation.drop_duplicates()
            df_unique_pps_installation['key']= 1
            merge_df = sqldf("select a.pps_id,a.installation_id,a.order_flow,b.ceil,b.interval_start,b.interval_end from df_unique_pps_installation  a inner join timeseriesdata b on a.key =b.key")
        else:
            merge_df = pd.DataFrame()
        return merge_df


    def get_item_pick(self, interval_start_date,interval_end_date, merge_df,flow_type):
        interval_start_date1 = interval_start_date
        interval_start_date1 = interval_start_date1.strftime('%Y-%m-%dT%H:%M:%SZ')
        interval_end_date = interval_end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        q = f"select installation_id,user_id, pps_id,uom_quantity_int,uom_quantity,value,order_flow_name,uom_requested,order_id  from item_picked where time>'{interval_start_date1}' and time<='{interval_end_date}'  order by time desc"
        df_itemput = pd.DataFrame(self.read_client.query(q).get_points())
        df_itemput = self.utilfunction.flow_event(df=df_itemput,site_name='msio')
        df_itemput= df_itemput[df_itemput['order_flow']==flow_type]
        df_itemput['time'] = pd.to_datetime(df_itemput['time'])
        df_itemput['uom_quantity_int'] = df_itemput.apply(self.utilfunction.calc_uom_quanity,axis=1)
        del df_itemput['uom_quantity']
        df_itemput = df_itemput.sort_values(by=['installation_id', 'pps_id', 'time'])
        df_itemput= df_itemput.reset_index()
        df_itemput['interval_start_date']= interval_start_date
        df_itemput['ceil'] = df_itemput.apply(self.utilfunction.ceil_data,axis=1)
        group_df =sqldf("select a.pps_id, max(b.user_id) as user_id, a.installation_id,a.interval_start,a.interval_end,a.order_flow,a.ceil, min(b.time) as Min_time,sum(b.uom_quantity_int) as sum_qty, sum(b.value) sum_value  from merge_df a left join  df_itemput b on  a.pps_id= b.pps_id and a.installation_id= b. installation_id and a.ceil= b.ceil and a.order_flow= b.order_flow group by a.pps_id, a.installation_id,a.interval_start,a.interval_end,a.ceil,a.order_flow")
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
            df2_flowevents['quantity_int'] = df2_flowevents.apply(self.utilfunction.calc_flowevent_quanity, axis=1)
            df2_flowevents['quantity_int'] = df2_flowevents['quantity_int'].apply(lambda x:int(x) if not pd.isna(x) else 0)
            del df2_flowevents['quantity']
            df2_flowevents = df2_flowevents.sort_values(by=['installation_id', 'pps_id', 'time'])
            df2_flowevents= df2_flowevents.reset_index()
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
                "select a.pps_id,max(b.user_loggedin) as user_id, a.installation_id,a.order_flow,a.interval_start,a.interval_end,a.ceil,min(b.time) time from merge_df a inner join  df2_flowevents b on  a.pps_id= b.pps_id and a.installation_id= b. installation_id and a.ceil= b.ceil group by a.pps_id, a.installation_id,a.interval_start,a.interval_end,a.order_flow")
        else:
            group_df = sqldf(
                "select 0 as user_id, a.pps_id, a.installation_id,a.order_flow,a.interval_start,a.interval_end,a.ceil from merge_df a  where 1=2 group by a.pps_id, a.installation_id,a.interval_start,a.interval_end,a.order_flow")
            group_df['time']=None
        return group_df

    def get_interval_stop_date(self):
        q = f"select interval_stop from  interval_throughput_msio where time>='{self.start_date}' order by time desc limit 1"
        try:
            df_time =pd.DataFrame(self.client.query(q).get_points())
            df_time['interval_stop'] = pd.to_datetime(df_time['interval_stop'])
            time = df_time['interval_stop'][0].to_pydatetime()
            return time
        except:
            pass
        return pd.to_datetime(self.start_date).to_pydatetime()

    def raw_data_calculation(self,flow_type):
        merge_df = self.get_merge_df(self.init_date_frame["interval_start_date"][0],self.init_date_frame["interval_end_date"][0], self.timeseriesdata,flow_type)
        if not merge_df.empty:
            df_item_pick = self.get_item_pick(self.init_date_frame["interval_start_date"][0],self.init_date_frame["interval_end_date"][0], merge_df,flow_type)
            df2_flowevents = self.get_flow_event(self.init_date_frame["interval_start_date"][0],self.init_date_frame["interval_end_date"][0], merge_df)
            zero_data_frame=df_item_pick[df_item_pick['sum_qty']==0]
            non_zero_data_frame=df_item_pick[df_item_pick['sum_qty']>0]
            df1=sqldf("select b.pps_id, max(a.user_id) as user_id, b.installation_id, a.order_flow, b.interval_start,b.interval_end,b.ceil,min(a.time) as Min_time_wfm, b.Min_time,0 as sum_qty,0 as sum_value,b.flag as flag from df2_flowevents a inner join zero_data_frame b on   a.pps_id= b.pps_id and a.installation_id= b. installation_id and a.ceil= b.ceil  and a.order_flow = b.order_flow group by b.installation_id, b.pps_id,b.interval_start,b.interval_end, b.flag,b.ceil,b.Min_time,a.order_flow" )
            if not df1.empty:
                df1["Min_time"] = df1[['Min_time','Min_time_wfm']].apply(lambda x: x['Min_time']if x['Min_time'] is not None   else x['Min_time_wfm'], axis=1)
            df2=sqldf("select b.pps_id, max(b.user_id) as user_id, b.installation_id, b.interval_start,b.order_flow,b.interval_end,b.ceil,min(b.Min_time) as Min_time_wfm, b.Min_time,0 as sum_qty,0 as sum_value,0  as flag from zero_data_frame b left join df2_flowevents a on   a.pps_id= b.pps_id and a.installation_id= b. installation_id and a.ceil= b.ceil and a.order_flow = b.order_flow where a.pps_id is null group by b.installation_id, b.pps_id,b.interval_start,b.interval_end,b.ceil,b.Min_time,b.order_flow")
            final_df= pd.concat([non_zero_data_frame, df1, df2], axis=0)
            del final_df['Min_time_wfm']
            if not final_df.empty:
                final_df= final_df.sort_values(by=['installation_id','pps_id','ceil'])
                final_df= final_df.reset_index()
                final_df['15_end']=final_df['ceil']-4
                final_df['30_end'] = final_df['ceil'] - 9
                final_df['60_end'] = final_df['ceil'] - 19
                final_df['15_end'] = final_df['15_end'].apply(lambda x: x if x >0 else None)
                final_df['30_end'] = final_df['30_end'].apply(lambda x: x if x >0 else None)
                final_df['60_end'] = final_df['60_end'].apply(lambda x: x if x >0 else None)
                final_df["pps_id_4"]= final_df['pps_id'].shift(+4)
                final_df["installation_id_4"] = final_df['installation_id'].shift(+4)
                final_df["pps_id_9"] = final_df['pps_id'].shift(+9)
                final_df["installation_id_9"] = final_df['installation_id'].shift(+9)
                final_df["pps_id_19"] = final_df['pps_id'].shift(+19)
                final_df["installation_id_19"] = final_df['installation_id'].shift(+19)
                final_df["msu_15_1"] =final_df[['pps_id_4','installation_id_4','pps_id','installation_id','ceil','15_end']].apply(lambda x: 1 if x["pps_id_4"] ==x["pps_id"] and x["installation_id_4"] ==x["installation_id"]  and x['ceil']>=x['15_end'] else 0, axis=1)
                final_df["msu_30_1"] =final_df[['pps_id_9','installation_id_9','pps_id','installation_id','ceil','30_end']].apply(lambda x: 1 if x["pps_id_9"] ==x["pps_id"] and x["installation_id_9"] ==x["installation_id"]  and x['ceil']>=x['30_end'] else 0, axis=1)
                final_df["msu_60_1"] =final_df[['pps_id_19','installation_id_19','pps_id','installation_id','ceil','60_end']].apply(lambda x: 1 if x["pps_id_19"] ==x["pps_id"] and x["installation_id_19"] ==x["installation_id"]  and x['ceil']>=x['60_end'] else 0, axis=1)
                final_df["msu_15_2"] =final_df[['pps_id_4','installation_id_4','pps_id','installation_id','ceil','15_end']].apply(lambda x: 1 if x["pps_id_4"] ==x["pps_id"] and x["installation_id_4"] ==x["installation_id"]   else 0, axis=1)
                final_df["msu_30_2"] =final_df[['pps_id_9','installation_id_9','pps_id','installation_id','ceil','30_end']].apply(lambda x: 1 if x["pps_id_9"] ==x["pps_id"] and x["installation_id_9"] ==x["installation_id"]   else 0, axis=1)
                final_df["msu_60_2"] =final_df[['pps_id_19','installation_id_19','pps_id','installation_id','ceil','60_end']].apply(lambda x: 1 if x["pps_id_19"] ==x["pps_id"] and x["installation_id_19"] ==x["installation_id"]   else 0, axis=1)

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
                final_df = final_df[(final_df.flag !=0)]
                if not final_df.empty:
                    temp_date = self.start_date + timedelta(hours=1)
                    final_df["start"]=temp_date
                    final_df['interval_start']= pd.to_datetime(final_df['interval_start'])
                    final_df["flag2"] = final_df.apply(self.utilfunction.date_compare, axis=1)
                    final_df = final_df[(final_df["flag2"])]
                    final_df= final_df.drop(['flag2','15_end','30_end', '60_end', 'pps_id_4', 'installation_id_4', 'pps_id_9',\
                        'installation_id_9', 'pps_id_19', 'installation_id_19', 'msu_15_1','msu_30_1', 'msu_60_1','msu_15', 'msu_30', 'msu_60','start', 'ceil','msu_15_2','msu_30_2','msu_60_2']     , axis=1)
                    lamdatest= lambda x: "Y" if x >0 else "N"
                    if not final_df.empty:
                        final_df.flag = final_df.flag.apply(lamdatest)
                        lamdatest= lambda x: "N" if pd.isna(x['sum_qty']) or x['sum_qty']==0 else x['flag']
                        final_df.flag = final_df.apply(lamdatest, axis=1)
                        final_df.rename(columns = {'sum_qty':'Sum_uom', 'sum_value':'Sum_value',
                                                      'interval_end':'interval_stop','Min_time':'time','flag':'msu_wait_flag'}, inplace = True)
                        final_df['interval_start'] = final_df['interval_start'].astype(str)
                        final_df['interval_stop'] = final_df['interval_stop'].astype(str)
                        # final_df['UPH_15min_value'] = final_df['UPH_15min_value'].astype(int)
                        # final_df['UPH_30min_value'] = final_df['UPH_30min_value'].astype(int)
                        # final_df['UPH_60min_value'] = final_df['UPH_60min_value'].astype(int)
                        # final_df['UPH_15min'] = final_df['UPH_15min'].astype(int)
                        # final_df['UPH_30min'] = final_df['UPH_30min'].astype(int)
                        # final_df['UPH_60min'] = final_df['UPH_60min'].astype(int)
                        final_df.time = pd.to_datetime(final_df.time)
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
                        final_df = final_df.set_index('time')
                        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                             port=self.tenant_info["write_influx_port"])
                        self.write_client.writepoints(final_df, "interval_throughput_msio", db_name=self.tenant_info["out_db_name"],
                                                      tag_columns=['pps_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

    # sd: 2022-11-09 07:00:00
    # ed: 2022-11-09 14:00:00
    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.utilfunction = CommonFunction()
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')
        
        check_start_date = self.client.get_start_date("interval_throughput_msio", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date)+timedelta(minutes=1) #corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select * from item_picked where time>'{check_start_date}' and time<='{check_end_date}' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.final_call1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.client.get_datetime_interval3("interval_throughput_msio", '1d', self.tenant_info)
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.final_call1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call1(self, end_date, **kwargs):
        self.start_date = self.client.get_start_date("interval_throughput_msio", self.tenant_info)
        self.start_date =pd.to_datetime(self.start_date).strftime("%Y-%m-%d %H:%M:%S")
        self.start_date=self.get_interval_stop_date()
        self.start_date = self.start_date - timedelta(hours=1)
        self.end_date = end_date
        self.start_date =self.start_date.astimezone(pytz.utc)
        self.end_date =self.end_date.astimezone(pytz.utc)

        # start_date=datetime(2022, 11, 9, 12, 30, 0, 119205)
        # start_date =start_date.astimezone(pytz.utc)
        # end_date=datetime(2022, 11, 9, 19, 30, 0, 119205)
        # end_date =end_date.astimezone(pytz.utc)

        self.init_date_frame = self.utilfunction.create_startdate_enddate_frame(self.start_date, self.end_date)
        #init_date_frame['interval_start_date']= init_date_frame['interval_start_date']- pd.Timedelta(hours = 1)
        #init_date_frame['start_date'] = init_date_frame['start_date'] - pd.Timedelta(hours=1)

        self.timeseriesdata = self.utilfunction.create_time_series_data(self.init_date_frame["interval_start_date"][0],self.init_date_frame["interval_end_date"][0], '3min')
        flow_types =['msio','non-msio']
        for flow_type in flow_types:
            self.raw_data_calculation(flow_type)

        return None


with DAG(
    'custom_Interval_throughput_msio',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'POC for Airflow as metrics computation pipeline orchestration tool',
    schedule_interval = '40 * * * *',
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
            if tenant['Active'] == "Y" and tenant['custom_Interval_throughput_msio'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "N"):
                final_task = PythonOperator(
                    task_id='custom_Interval_throughput_msio_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(custom_IntervalThroughput_msio().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"Adidas", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='custom_Interval_throughput_msio',
            provide_context=True,
            python_callable=functools.partial(custom_IntervalThroughput_msio().final_call,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

