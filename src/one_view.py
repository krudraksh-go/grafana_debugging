import pandas as pd
import json

import gspread
import pytz
import numpy as np
from math import ceil
import os
import airflow
from airflow import DAG
from utils.CommonFunction import SingleTenantDAGBase, MultiTenantDAG,CommonFunction
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone
from config import (
    INFLUX_DATETIME_FORMAT,
    GOOGLE_AUTH_JSON,
    CONFFILES_DIR
)

####################### Config #######################

dag = DAG(
    "one_view",
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval='15 7 */1 * *',
    default_args=CommonFunction().get_default_args_for_dag(),
    catchup=False,
)

utc = pytz.UTC


####################### Helper functions #######################
def get_label(scale):
    if scale >= 1 and scale <= 3:
        return 'D'
    elif scale == 4:
        return 'N'
    elif scale == 5:
        return 'P'
    else:
        return 'Nothing'


def get_agg_label(row):
    if not row['percent_P'] and not row['percent_D'] and not row['percent_N']:
        return 'Nothing'
    else:
        if row['percent_D'] >= 0.75:
            return 'Detractor'
        elif row['percent_P'] >= 0.75:
            return 'Promoter'
        else:
            return 'Neutral'


####################### Task definations #######################
class OneViewAYX():
    def pick_previous_raw_data(self, df, field):
        for i in df.index:
            if i >= 0 and i != df.index[-1]:
                if df['copy_flag'][i] == '1' and not pd.isna(df[field][i-1]):
                    df[field][i]=df[field][i-1]
                else:
                    df[field][i] = df[field][i]
        return df

    def set_value_one(self,df ,outfield):
        for i in df.index:
            if i >= 0 and i != df.index[-1]:
                if df[outfield][i] > 1:
                    df[outfield][i] = 1
        return df

    def monthly_weekly_raw_data(self, df, field1,field2,outfield,rowcount):
        for i in df.index:
            if i >= 0 and i != df.index[-1]:
                sum_field1=0.0
                sum_field2=0.0
                for j in range(rowcount):
                    if i+j < df.index[-1]:
                        sum_field1=sum_field1+ df[field1][i+j]
                        sum_field2 = sum_field2 + df[field2][i+j]
                if sum_field2!=0:
                    df[outfield][i]=round(sum_field1/sum_field2,2)
                else:
                    df[outfield][i] = 0
        return df

    def c1(self, df, field,out_field):
        for i in df.index:
            if i >= 0 and i+1 < df.index[-1]:
                if df[field][i] == 0:
                    df[out_field][i]=0
                elif df[field][i+1]==0:
                    df[out_field][i] = 1
                else:
                    df[out_field][i] = round(df[field][i]/df[field][i+1],2)
        return df

    def c2(self, df, field,out_field):
        for i in df.index:
            if i >= 0 and i != df.index[-1] and i+7 < df.index[-1]:
                if df[field][i] == 0:
                    df[out_field][i]=0
                elif df[field][i+7]==0:
                    df[out_field][i] = 1
                else:
                    df[out_field][i] =round(df[field][i]/df[field][i+7], 2)
        return df

    def check_value(self,value):
        if str(value)=='0':
            return 0
        else:
            return 1
    def c3(self, df, field,out_field):
        for i in df.index:
            if i >= 0 and i != df.index[-1] and i+28 < df.index[-1]:
                if df[field][i] == 0:
                    df[out_field][i]=0
                elif df[field][i+7]==0 and df[field][i+14]==0 and df[field][i+21]==0 and df[field][i+28]==0:
                    df[out_field][i] = 1
                else:
                    sumval=df[field][i+7]+df[field][i+14]+df[field][i+21]+df[field][i+28]
                    count=self.check_value(df[field][i+7])+self.check_value(df[field][i+14])+self.check_value(df[field][i+21])+self.check_value(df[field][i+28])
                    if count!=0:
                        sumval=sumval/count

                    df[out_field][i] =round(df[field][i]/sumval, 2)
        return df

    def c4(self, df, field,out_field):
        for i in df.index:
            if i >= 0 and i != df.index[-1] and i+7 < df.index[-1]:
                if df[field][i] == 0:
                    df[out_field][i]=0
                elif df[field][i+1]==0 and df[field][i+2]==0 and df[field][i+3]==0 and df[field][i+4]==0 and df[field][i+5]==0 and df[field][i+6]==0 and df[field][i+7]==0:
                    df[out_field][i] = 1
                else:
                    sumval=df[field][i+1]+df[field][i+2]+df[field][i+3]+df[field][i+4]+df[field][i+5]+df[field][i+6]+df[field][i+7]
                    count=self.check_value(df[field][i+1])+self.check_value(df[field][i+2])\
                          +self.check_value(df[field][i+3])+self.check_value(df[field][i+4]) \
                          + self.check_value(df[field][i + 5]) + self.check_value(df[field][i + 6])+ self.check_value(df[field][i + 7])
                    if count!=0:
                        sumval=sumval/count

                    df[out_field][i] =round(df[field][i]/sumval, 2)
        return df

    def c5(self, df, field,out_field):
        for i in df.index:
            if i >= 0 and i != df.index[-1] and i+28 < df.index[-1]:
                sumval=0.0
                count=0
                if df[field][i] == 0:
                    df[out_field][i]=0
                else:
                    for j in range(1,29):
                        sumval=sumval+df[field][i+j]
                        count=count+self.check_value(df[field][i+j])
                    if count!=0:
                        sumval=sumval/count
                    if sumval!=0:
                        df[out_field][i] =round(df[field][i]/sumval, 2)
                    else:
                        df[out_field][i] =1 #set None 1
        return df

    def c6(self, df, field,out_field):
        for i in df.index:
            if i >= 0 and i != df.index[-1] and i+14 < df.index[-1]:
                sumval=0.0
                count=0
                if df[field][i] == 0:
                    df[out_field][i]=0
                else:
                    for j in range(0,14):
                        sumval=sumval+df[field][i+j]
                        count=count+self.check_value(df[field][i+j])
                    if count!=0:
                        sumval=sumval/count

                    if sumval!=0:
                        df[out_field][i] =round(df[field][i]/sumval, 2)
                    else:
                        df[out_field][i] =1 #set None 1
        return df

    def c7(self, df, field,out_field):
        for i in df.index:
            if i >= 0 and i != df.index[-1] and i+35 < df.index[-1]:
                sumval=0.0
                count=0
                if df[field][i] == 0:
                    df[out_field][i]=0
                else:
                    for j in range(1,36):
                        sumval=sumval+df[field][i+j]
                        count=count+self.check_value(df[field][i+j])
                    if count!=0:
                        sumval=sumval/count

                    if sumval!=0:
                        df[out_field][i] =round(df[field][i]/sumval, 2)
                    else:
                        df[out_field][i] =1 #set None 1
        return df

    def c8(self, df, field,out_field):
        for i in df.index:
            if i >= 0 and i != df.index[-1] and i+13 < df.index[-1]:
                sumval=0.0
                count=0
                for j in range(0, 7):
                    sumval = sumval + df[field][i + j]
                if sumval==0:
                    df[out_field][i] =0
                else:
                    for j in range(7,14):
                        sumval=sumval+df[field][i+j]
                    if sumval==0:
                        df[out_field][i] =1
                    else:
                        for j in range(0,14):
                            sumval=sumval+df[field][i+j]
                            count=count+self.check_value(df[field][i+j])
                        if count!=0:
                            sumval=sumval/count
                        if sumval!=0:
                            df[out_field][i] =round(df[field][i]/sumval, 2)
                        else:
                            df[out_field][i] =1 #set None 1
        return df

    def cal_func(self,func_number,df,field,out_field):
        df[out_field]=1.0
        if func_number==1:
            df= self.c1(df,field,out_field)
        elif func_number==2:
            df =self.c2(df, field, out_field)
        elif func_number==3:
            df=self.c3(df, field, out_field)
        elif func_number==4:
            df=self.c4(df, field, out_field)
        elif func_number==5:
            df=self.c5(df, field, out_field)
        elif func_number==6:
            df=self.c6(df, field, out_field)
        elif func_number==7:
            df=self.c7(df, field, out_field)
        elif func_number==8:
            df =self.c8(df, field, out_field)

        df =self.set_value_one(df =df,outfield=out_field)

        return df

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df

    def get_datetime_interval2(self):
        influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        start_datetime = influx.fetch_one("""select * from OneView_data_v2 
                      where site='{site}'  order by time desc limit 1""".format(
            site=self.site.get('name')))
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
            start_datetime = start_datetime + timedelta(days=1)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=1)
        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = datetime.now(timezone.utc)
        timeseriesdata = self.create_time_series_data2(start_datetime,end_datetime, '1d')
        return timeseriesdata

    def get_datetime_interval3(self, end_date):
        influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        start_datetime = influx.fetch_one("""select * from OneView_data_v2 
                      where site='{site}'  order by time desc limit 1""".format(
            site=self.site.get('name')))
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=3)
        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = end_date
        timeseriesdata = self.create_time_series_data2(start_datetime,end_datetime, '1d')
        return timeseriesdata

    def OneView_ayx(self, **kwargs):
        self.CommonFunction = CommonFunction()
        daterange = self.get_datetime_interval2()
        for i in daterange.index:
            self.start_date=daterange['start_date'][i]
            self.end_date=daterange['end_date'][i]
            self.OneView_ayx1()

    def OneView_ayx_by_kpi(self,end_date):
        self.CommonFunction = CommonFunction()
        daterange = self.get_datetime_interval3(end_date)
        for i in daterange.index:
            self.start_date=daterange['start_date'][i]
            self.end_date=daterange['end_date'][i]
            print(f"oneview:startdate:{self.start_date} and enddate:{self.end_date}")
            self.OneView_ayx1()

    def OneView_ayx1(self):
        # quarter_info = self.get_quarter_info()
        # quarter_start_date = datetime.strptime(
        #     f"{quarter_info.get('quarter_start_date')} {quarter_info.get('hour')}:{quarter_info.get('minute')}:00",
        #     INFLUX_DATETIME_FORMAT
        # )



        influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        isvalid = influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
        if isvalid:
            start_date = self.start_date
            start_date=pd.to_datetime(start_date,utc=True)
            orig_start_date=start_date
            end_date= self.end_date
            end_date = pd.to_datetime(end_date, utc=True)
            start_date= start_date-timedelta(days=37)
            site=self.site.get('name')
            init_date_frame = self.CommonFunction.create_time_series_data2(start_date, end_date, '1d')
            start_date=start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
            query = f"select * from all_site_kpi_new_ayx where site='{site}' and time >= '{start_date}' and time <= '{end_date}'"
            query2 = f"select * from daily_site_kpi_ayx where site='{site}' and time >= '{start_date}' and time <= '{end_date}'"

            init_date_frame=init_date_frame.rename(columns = {'interval_start':'start','interval_end':'end'})
            del init_date_frame['end']

            # SHEET_ID = '1C-XCy5DDr1QHx5789k-e0PJXEt-wDfgSc1rjEzdYTiA'
            # SHEET_NAME = 'OneView_Promised'
            # gc = gspread.service_account(GOOGLE_AUTH_JSON)
            # spreadsheet = gc.open_by_key(SHEET_ID)
            # worksheet = spreadsheet.worksheet(SHEET_NAME)
            # rows = worksheet.get_all_values()
            # df_sheet = pd.DataFrame(rows)
            # expected_headers = df_sheet.iloc[1]
            # df_sheet.columns = expected_headers
            # print(df_sheet)
            all_sites_data_df = influx.fetch_all(query)
            daily_sites_data_df = influx.fetch_all(query2)
            print(all_sites_data_df)
            print(daily_sites_data_df)
            print(init_date_frame)
            if daily_sites_data_df.empty:
                return

            if all_sites_data_df.empty:
                return
            path = f''
            path = os.path.join(CONFFILES_DIR, path)

            #df_sheet.to_csv(path+"df_sheet.csv" ,index=False, header=True)
            all_sites_data_df.reset_index(inplace=True)
            daily_sites_data_df.reset_index(inplace=True)
            all_sites_data_df=all_sites_data_df.rename(columns = {'index':'time',\
                                'total_errors_excl_hardemg':'total_errors_excluding_HE_and_SS'})
            daily_sites_data_df = daily_sites_data_df.rename(columns={'index': 'time'})
            all_sites_data_df.sort_values('time', ascending=False, inplace=True)
            all_sites_data_df['time_old']=all_sites_data_df['time'].apply(lambda x:x.strftime("%Y-%m-%d"))
            all_sites_data_df['breached_orders_score'] = all_sites_data_df['breached_orders'].apply(lambda x:round(1-float(x),2))
            all_sites_data_df.drop_duplicates(subset=['time_old'], keep='first')

            #all_sites_data_df.to_csv(path+"all_sites_data_df.csv",index=False, header=True)
            #daily_sites_data_df.to_csv(path+"daily_sites_data_df.csv",index=False, header=True)
            # init_date_frame.to_csv(path+"init_date_frame.csv",index=False, header=True)

            daily_sites_data_df['time_old'] = daily_sites_data_df['time']
            daily_sites_data_df['time_old'] = daily_sites_data_df['time'].apply(lambda x: x.strftime("%Y-%m-%d"))
            init_date_frame['start'] = init_date_frame['start'].apply(lambda x: x.strftime("%Y-%m-%d"))
            # daily_sites_data_df.drop(['COUNT_UPH_60min','COUNT_perface_pick_operator_working_time','COUNT_perface_put_operator_working_time','COUNT_picks_per_face','COUNT_put_UPH_60min','COUNT_put_per_face','COUNT_put_rack_to_rack','RTR_Count','RTR_Sum', \
            #                           'SUM_UPH_60min','SUM_perface_pick_operator_working_time',\
            #                           'SUM_perface_put_operator_working_time','SUM_picks_per_face','SUM_put_UPH_60min',\
            #                           'SUM_put_per_face','SUM_put_rack_to_rack','items_put_srms','items_to_be_put_srms',\
            #                           'put_exceptions_srms'], axis=1, inplace=True)
            l = ['COUNT_UPH_60min','COUNT_perface_pick_operator_working_time','COUNT_perface_put_operator_working_time','COUNT_picks_per_face','COUNT_put_UPH_60min','COUNT_put_per_face','COUNT_put_rack_to_rack','RTR_Count','RTR_Sum', \
                                      'SUM_UPH_60min','SUM_perface_pick_operator_working_time',\
                                      'SUM_perface_put_operator_working_time','SUM_picks_per_face','SUM_put_UPH_60min',\
                                      'SUM_put_per_face','SUM_put_rack_to_rack','items_put_srms','items_to_be_put_srms'\
                                      'put_exceptions_srms','active_bots','active_chargers','avg_item_per_tx','avg_line_count','avg_utilization','avg_utilization_percent','Bots_in_maintenance','CBM',
                                        'CBM_put_actual','created_order_breached','items_to_be_put','new_put_request','operator_working_time','order_completed','order_completed_breached',
                                        'order_created','order_created_at_risk','order_tasked','order_tasked_at_risk','order_tasked_breached','orders_created_nonbreach',
                                        'perface_pick_operator_working_time','put_UPH_60min','qty_per_orderline','RTR_avg_90','RTR_avg_95','RTR_avg_99','slots_used',
                                        'time_to_put_one_item_value','Total_Bots','total_chargers','total_exceptions','total_item_put','total_put_request','total_rack_pr','total_warehouse_cap_CBM','tx_expectation','UPH_15min','UPH_30min','UPH_60min','UPH_RTR','UPH_RTR_90','UPH_RTR_95','UPH_RTR_99'
                                        ]
            for col in l:
                if col in daily_sites_data_df.columns:
                    del daily_sites_data_df[col]
            daily_sites_data_df.sort_values('time', ascending=False, inplace=True)
            daily_sites_data_df.drop_duplicates(subset=['time_old'], keep='first')
            daily_sites_data_df=pd.merge(all_sites_data_df[['time','time_old','copy_flag']],daily_sites_data_df,on=['time','time_old'],how='left')
            daily_sites_data_df=self.pick_previous_raw_data(daily_sites_data_df,'RTR_avg')
            daily_sites_data_df = self.pick_previous_raw_data(daily_sites_data_df, 'operator_working_time_2')
            daily_sites_data_df = self.pick_previous_raw_data(daily_sites_data_df, 'perface_put_operator_working_time')
            daily_sites_data_df = self.pick_previous_raw_data(daily_sites_data_df, 'picks_per_face')
            daily_sites_data_df = self.pick_previous_raw_data(daily_sites_data_df, 'put_per_face')
            daily_sites_data_df = self.pick_previous_raw_data(daily_sites_data_df, 'put_rack_to_rack')
            daily_sites_data_df = self.pick_previous_raw_data(daily_sites_data_df, 'site')
            all_sites_data_df= self.CommonFunction.reset_index(all_sites_data_df)
            if 'index' in all_sites_data_df.columns:
                del all_sites_data_df['index']
            if 'index' in daily_sites_data_df.columns:
                del daily_sites_data_df['index']

            daily_sites_data_df = self.CommonFunction.reset_index(daily_sites_data_df)
            for j in range(1,9):
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df,field='entity_picked',out_field=f'SE_OB_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='UPH', out_field=f'SE_UPH_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='entity_put', out_field=f'SE_IB_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='barcode_per_stop', out_field=f'SA_BpS_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='butler_uptime', out_field=f'SA_BotUp_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='charger_uptime', out_field=f'SA_ChargerUp_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='sw_uptime', out_field=f'SA_SWUp_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='put_UPH', out_field=f'SE_put_UPH_C{j}')

                daily_sites_data_df = self.cal_func(func_number=j, df=daily_sites_data_df, field='RTR_avg', out_field=f'SE_RTR_Pick_C{j}')
                daily_sites_data_df = self.cal_func(func_number=j, df=daily_sites_data_df, field='operator_working_time_2', out_field=f'SE_OWT_Pick_C{j}')
                daily_sites_data_df = self.cal_func(func_number=j, df=daily_sites_data_df, field='picks_per_face', out_field=f'SE_PPF_Pick_C{j}')
                daily_sites_data_df = self.cal_func(func_number=j, df=daily_sites_data_df, field='put_rack_to_rack', out_field=f'SE_RTR_Put_C{j}')
                daily_sites_data_df = self.cal_func(func_number=j, df=daily_sites_data_df, field='put_per_face', out_field=f'SE_PPF_Put_C{j}')

            for j in range(1, 8):
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='operator_NPS_pick', out_field=f'NPS_operator_pick_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='operator_NPS_put', out_field=f'NPS_operator_put_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='operator_NPS_audit', out_field=f'NPS_operator_audit_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='ops_support_NPS', out_field=f'NPS_ops_support_C{j}')
                all_sites_data_df = self.cal_func(func_number=j, df=all_sites_data_df, field='WH_Score', out_field=f'NPS_WhM_Score_C{j}')

            all_sites_data_df['SA_Lat_daily']=all_sites_data_df.apply(lambda x:round(x['latency_count_lessthan_500']/x['total_latency_count'],2) if x['total_latency_count']!=0 else 0.0 , axis=1)
            all_sites_data_df['SA_Lat_monthly'] = 1.0
            all_sites_data_df['SA_Lat_weekly'] = 1.0
            all_sites_data_df['breached_orders_W'] = 1.0
            all_sites_data_df['breached_orders_M'] = 1.0
            all_sites_data_df=self.monthly_weekly_raw_data(all_sites_data_df,'latency_count_lessthan_500','total_latency_count','SA_Lat_monthly',28)
            all_sites_data_df=self.monthly_weekly_raw_data(all_sites_data_df,'latency_count_lessthan_500','total_latency_count','SA_Lat_weekly',7)
            all_sites_data_df=self.monthly_weekly_raw_data(all_sites_data_df,'order_breached_completed','orders_completed','breached_orders_W',7)
            all_sites_data_df=self.monthly_weekly_raw_data(all_sites_data_df,'order_breached_completed','orders_completed','breached_orders_M',28)
            all_sites_data_df['breached_orders_W_score'] = all_sites_data_df['breached_orders_W'].apply(lambda x:1-float(x))
            all_sites_data_df['breached_orders_M_score'] = all_sites_data_df['breached_orders_M'].apply(lambda x:1-float(x))
            all_sites_data_df=pd.merge(all_sites_data_df,init_date_frame , left_on ='time_old', right_on='start',how='left')
            # all_sites_data_df=pd.merge(all_sites_data_df,df_sheet, left_on ='site', right_on='excel_names' , on ='left')
            daily_sites_data_df=pd.merge(daily_sites_data_df,init_date_frame , left_on ='time_old', right_on='start',how='left')
            # daily_sites_data_df = pd.merge(daily_sites_data_df, df_sheet, left_on='site', right_on='excel_names', on='left')
            # del daily_sites_data_df['excel_names']
            # del all_sites_data_df['excel_names']
            daily_sites_data_df = daily_sites_data_df.rename(columns={'RTR_avg': 'RTR_Pick', 'operator_working_time_2': 'OWT_Pick', \
                                                                      'perface_put_operator_working_time': 'OWT_Put','picks_per_face': 'PPF_Pick',
                                                                      'put_per_face':'PPF_Put','put_rack_to_rack':'RTR_Put'})
            l=['site','time','time_old','copy_flag']
            for col in l:
                if col in daily_sites_data_df.columns:
                    del daily_sites_data_df[col]
            final_df= pd.merge(all_sites_data_df,daily_sites_data_df,on='start',how='left')
            l=['start','time_old','total_put_expectations']
            for col in l:
                if col in final_df.columns:
                    del final_df[col]

#            all_sites_data_df.to_csv(path+"all_sites_data_df.csv",index=False, header=True)
#            daily_sites_data_df.to_csv(path+"daily_sites_data_df.csv",index=False, header=True)
#            final_df.to_csv(path + "final_df.csv", index=False, header=True)
            field_list=['RTR_Pick','OWT_Pick','OWT_Put','PPF_Pick','PPF_Put','RTR_Put',\
                        'Total_rack_presentation_pick','Total_rack_presentation_put','barcode_travelled',\
                        'total_errors','total_errors_excluding_HE_and_SS','operator_minutes_pick','operator_minutes_put','uom_per_opmin',\
                        'Downtime','Sum_charging_time','Sum_idle_time','Sum_online_time','Sum_pps_task_time','Sum_total_kpi','Sum_total_time']
            for field in field_list:
                try:
                    final_df[field]=final_df[field].fillna(0)
                except Exception as e:
                    print(e)
            if 'index' in final_df.columns:
                    del final_df['index']

            fields= ['breached_orders_W','breached_orders_M','SA_Lat_daily','SA_Lat_monthly','SA_Lat_weekly','breached_orders_W_score','breached_orders_M_score']
            for f in fields:
                final_df = self.set_value_one(df=final_df, outfield=f)

            #final_df.sort_values('tim', ascending=True, inplace=True)
            final_df['time'] = pd.to_datetime(final_df['time'])
            orig_start_date=pd.to_datetime(orig_start_date)
            final_df['latest_date_check']= final_df['time'].apply(lambda x: True if (x -orig_start_date).total_seconds() >=0 else False)
            final_df = final_df[(final_df["latest_date_check"])]

            for f in final_df.select_dtypes(include=np.integer).columns.tolist():
                final_df[f]=final_df[f].astype(float)

            fields = ['Downtime', 'NPS_ops_support', 'NPS_ops_support_C1', 'NPS_ops_support_C2', 'NPS_ops_support_C3',
                      'NPS_ops_support_C4', 'NPS_ops_support_C5', 'NPS_ops_support_C6', 'NPS_ops_support_C7','PPF_Pick','PPF_Put','RTR_Pick','RTR_Put']
            for f in fields:
                final_df[f] = final_df[f].astype(float)
            del final_df['latest_date_check']
            #final_df.to_csv(path + "final_df.csv", index=False, header=True)
            final_df = final_df.set_index('time')
            isvalid = influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
            if isvalid:
                success = influx.write_all(final_df, 'OneView_data_v2', format="dataframe", dag_name=os.path.basename(__file__), site_name=self.site.get('name'), tag_columns=['site'])
        return



#
# MultiTenantDAG(
#     dag,
#     [
#         'OneView_ayx',
#     ],
#     [
#     ],
#     OneViewAYX
# ).create()
