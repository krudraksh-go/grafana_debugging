# from influxdb import InfluxDBClient
# from influxdb import DataFrameClient
# import pandas as pd
# import numpy as np
# import datetime as dt
# from datetime import timedelta, datetime,timezone
# from pandasql import sqldf
# client=InfluxDBClient(host='10.110.67.4',port=8086)
# client.switch_database('GreyOrange')
# q="select * from slots_ageing_ayx where time > now()-5h order by time desc limit 1"
# cron_run_at=pd.DataFrame(client.query(q).get_points())
# latest_inventory_transacion = cron_run_at["last_transaction_time"][0]
# start_date = cron_run_at["cron_ran_at"][0]
# end_date = dt.now(timezone.utc)

# import psycopg2
# import pandas as pd
# import numpy as np
# import datetime
# connection = psycopg2.connect(user="altryx_read", password="YLafmbfuji$@#45!", host="10.110.67.2", port="5432", database="butler_dev")
# cursor = connection.cursor()
# cursor.execute(f"select * from inventory_transaction_archives where time> '{latest_inventory_transacion}' order by time desc")
# #how to get latest time and cron ran at from old df wherever it is logged
# Orders = cursor.fetchall()
# df = pd.DataFrame(Orders,columns=['ID','transaction_id','external_service_request_id','transaction_type','request_id','event_name','user_name','slot','tpid','item_id','old_uom','delta_uom','new_uom','process_id','time','pps_id','ppsbin_id'])
# df = df.loc[(df['transaction_type'] == 'pick') | (df['transaction_type'] == 'put') | (df['transaction_type'] == 'audit')]
# #check if new data is present by comparing time. If not present push as it is. if present->
# q=f"select * from slots_ageing_ayx where time>'{start_date}'  and remaining_uom>0  order by time desc"
# df_prev=pd.DataFrame(client.query(q).get_points())


# df['rack_id'] = df['slot'].str.split('.', expand=True)[0]
# df['rack_face'] = df['slot'].str.split('.', expand=True)[1]
# df['old_uom'] = df.old_uom.astype(str).str.extract('(\d+)}$', expand=False)
# df['new_uom'] = df.new_uom.astype(str).str.extract('(\d+)}$', expand=False)
# df['delta_uom'] = df.delta_uom.astype(str).str.extract('(\d+)}$', expand=False)
# df['unique_id'] = df['item_id'] + '_' + df['slot']
# df['start_date_time']= start_date
# df['end_date_time']= datetime.now()
# df = df.sort_values(by=['time'],ascending=False)
# current_transaction_time= df['time'][0]
# if latest_inventory_transacion>current_transaction_time:
#     current_transaction_time=latest_inventory_transacion

# #df_prev=df #get df_prev from historic cron
# df_prev=df_prev.loc[df_prev['remaining_uom'] > 0]
# df_4b=pd.merge(df_prev,df,how='left',on='unique_id')
# df_4b['untouched_ageing']=pd.Timedelta(df_4b.latest_inventory_transaction - df_4b.last_transaction_time).seconds / 3600.0
# df_4b['slot_ageing']=pd.Timedelta(df_4b.latest_inventory_transaction - df_4b.first_put_time).seconds / 3600.0
# dfb=pd.merge(df,df_prev,how='left',on='unique_id') #go through historic
# df_case1 = dfb.loc[(dfb['transaction_type'] == 'put') & (dfb['old_uom'] == 0)]
# dfb = dfb.loc[(dfb['transaction_type'] != 'put') | (dfb['old_uom'] != 0)]
# dfb['right_time']=dfb['time']
# dfb['right_transaction_type']=dfb['transaction_type']
# dfb['right_new_uom']=dfb['new_uom']
# dfb['right_rack_id']=dfb['rack_id']
# dfb['right_rack_face']=dfb['rack_face']
# dfb['right_slot']=dfb['slot']
# dfb.drop(columns=['time','trasaction_type','new_uom','rack_id','rack_face','slot'])
# df_inner=pd.merge(dfb,df_case1,how='inner')
# df_case3=pd.merge(df_case1,dfb,how='left')
# df_case2=pd.merge(df_case1,dfb,how='right')
# df_case11 = df_inner.loc[(df_inner['time'] > df_inner['right_time'])]
# df_case12 = df_inner.loc[(df_inner['time'] < df_inner['right_time'])]
# df_case12['first_put_uom']=df_case12['new_uom']
# df_case12['first_put_time']=df_case12['time']
# df_case12['transaction_type']=df_case12['right_transaction_type']
# df_case12['remaining_uom']=df_case12['right_new_uom']
# df_case12['transaction_time']=df_case12['right_time']
# df_case12.drop(columns=['old_uom','delta_uom','pps_id','right_slot','right_rack_id','right_rack_face'])
# df_case12['Flag']="Touched"
# df_case12 = df_case12[(df_case12['remaining_uom'] > 0)]
# df_case11['first_put_uom']=df_case11['new_uom']
# df_case11['first_put_time']=df_case11['time']
# df_case11['Flag']="untouched"
# df_case3['first_put_time']=df_case3['time']
# df_case3['first_put_uom']=df_case3['new_uom']
# df_case3['remaining_uom']=df_case3['first_put_uom']
# df_case3['Flag']="untouched"
# df_case3 = df_case3[(df_case3['remaining_uom'] > 0)]
# df_case2['Flag']="untouched"
# df_case2['first_put_uom']=df_case2['new_uom']
# df_case2['remaining_uom']=df_case2['first_put_uom']
# df_case2['first_put_time']=df_case2['time']
# df_case2 = df_case2[(df_case2['remaining_uom'] > 0)]
# df_4a=pd.merge(df_case11,df_case12,how='outer')
# df2=pd.merge(df,df_case2,how='outer')
# df_4a=pd.merge(df2,df_case3,how='outer')
# cursor = connection.cursor()
# cursor.execute("select * from inventory_transaction_archives order by time desc limit 1")
# Orders = cursor.fetchall()
# df_time = pd.DataFrame(Orders,columns=['ID','transaction_id','external_service_request_id','transaction_type','request_id','event_name','user_name','slot','tpid','item_id','old_uom','delta_uom','new_uom','process_id','time','pps_id','ppsbin_id'])
# latest=df_time.time #giving NaN to rest after 1st row
# df_4a['latest_inventory_transaction'] = latest
# def check_if_null(a, b):
#         if a.isna:
#             val= b
#         else:
#             val= a
#         return int(val)

# df_4a.transaction_time= check_if_null(df_4a.transaction_time,df_4a.first_put_time)
# df_4a['untouched_ageing']=pd.Timedelta(df_4a.transaction_time - df_4a.latest_inventory_transaction).seconds / 3600.0
# df_4a['slot_ageing']=pd.Timedelta(df_4a.first_put_time - df_4a.latest_inventory_transaction).seconds / 3600.0
# #select rack_id and rack_type from data_rack and join on rack_id to find rack_type
# df_4a['cron_ran_at']=datetime.now()
# #inner join
# df_joined=pd.merge(df_prev,df,how='inner',on='unique_id')
# m1=df_joined['unique_id'].eq(df_joined['unique_id'].shift(-1))
# m2=df_joined['transaction_type'].eq('put')
# m3=df_joined['transaction_type'].shift(-1).eq('pick')
# m4=df_joined['new_uom'].shift(-1).eq(0)
# m5=df_joined['old_uom'].eq(0)
# df_joined['carry_flag']=(m1&m2&m3&m4&m5).astype(int)
# m1=df_joined['unique_id'].eq(df_joined['unique_id'].shift(-1))
# m2=df_joined['transaction_type'].eq('pick')
# m3=df_joined['transaction_type'].shift(-1).eq('put')
# m4=df_joined['old_uom'].shift(-1).eq(0)
# m5=df_joined['new_uom'].eq(0)
# df_joined['close_flag']=(m1&m2&m3&m4&m5).astype(int)
# df_closeone = df_joined.loc[(df_joined['close_flag'] == 1)]
# df_closezero = df_joined.loc[(df_joined['close_flag'] == 0)]
# df_carryone = df_closezero.loc[(df_closezero['carry_flag'] == 1)]
# df_carryzero = df_closezero.loc[(df_closezero['carry_flag'] == 0)]
# df_carryone.rename(columns = {'transaction_type':'last_transaction_event', 'new_uom':'first_put_uom', 'time':'transaction_time'}, inplace = True)
# df_carryzero.rename(columns = {'transaction_type':'last_transaction_event', 'new_uom':'remaining_uom', 'time':'transaction_time'}, inplace = True)
# df_left=pd.merge(df_carryzero,df_carryone,how='left')
# df_right=pd.merge(df_carryzero,df_carryone,how='right')
# df_right.rename(columns = {'first_put_uom':'remaining_uom', 'first_put_time':'transaction_time'}, inplace = True)
# df_right['flag']='untouched'
# df_left['flag']='touched'
# df_carryone['right_time']=df_carryone['time']
# df_carryone['right_transaction_type']=df_carryone['transaction_type']
# df_carryone['right_new_uom']=df_carryone['new_uom']
# df_carryone['right_rack_id']=df_carryone['rack_id']
# df_carryone['right_rack_face']=df_carryone['rack_face']
# df_carryone['right_slot']=df_carryone['slot']
# df_carryone.drop(columns=['time','trasaction_type','new_uom','rack_id','rack_face','slot'])
# df_inner=pd.merge(df_carryzero,df_carryone,how='inner')
# df_case11 = df_inner.loc[(df_inner['time'] > df_inner['right_time'])]
# df_case12 = df_inner.loc[(df_inner['time'] < df_inner['right_time'])]
# df_case12['first_put_uom']=df_case12['new_uom']
# df_case12['first_put_time']=df_case12['time']
# df_case12['transaction_type']=df_case12['right_transaction_type']
# df_case12['remaining_uom']=df_case12['right_new_uom']
# df_case12['transaction_time']=df_case12['right_time']
# df_case12.drop(columns=['old_uom','delta_uom','pps_id','right_slot','right_rack_id','right_rack_face'])
# df_case12['Flag']="Touched"
# df_case12 = df_case12[(df_case12['remaining_uom'] > 0)]
# df_case11['first_put_uom']=df_case11['new_uom']
# df_case11['first_put_time']=df_case11['time']
# df_case11['remaning_uom']=df_case11['first_put_uom']
# df_case11['Flag']="untouched"
# df_merged=pd.merge(df_case11,df_case12,how='outer')
# df_merged=pd.merge(df_merged,df_left,how='outer')
# df_4c=pd.merge(df_merged,df_right,how='outer')
# df_4a=pd.merge(df_4a,df_4b,how='outer')
# df_final=pd.merge(df_4a,df_4c,how='outer')