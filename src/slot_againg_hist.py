# import psycopg2
# import pandas as pd
# import numpy as np
# import datetime
# connection = psycopg2.connect(user="altryx_read", password="YLafmbfuji$@#45!", host="10.110.67.2", port="5432", database="butler_dev")
# cursor = connection.cursor()
# cursor.execute("select * from inventory_transaction_archives limit 1000")
# Orders = cursor.fetchall()
# df = pd.DataFrame(Orders,columns=['ID','transaction_id','external_service_request_id','transaction_type','request_id','event_name','user_name','slot','tpid','item_id','old_uom','delta_uom','new_uom','process_id','time','pps_id','ppsbin_id'])
# df = df.loc[(df['transaction_type'] == 'pick') | (df['transaction_type'] == 'put') | (df['transaction_type'] == 'audit')]
# df['rack_id'] = df['slot'].str.split('.', expand=True)[0]
# df['rack_face'] = df['slot'].str.split('.', expand=True)[1]
# df['old_uom'] = df.old_uom.astype(str).str.extract('(\d+)}$', expand=False)
# df['new_uom'] = df.new_uom.astype(str).str.extract('(\d+)}$', expand=False)
# df['delta_uom'] = df.delta_uom.astype(str).str.extract('(\d+)}$', expand=False)
# df['unique_id'] = df['item_id'] + '_' + df['slot']
# df_case1 = df.loc[(df['transaction_type'] == 'put') & (df['old_uom'] == 0)]
# df = df.loc[(df['transaction_type'] != 'put') | (df['old_uom'] != 0)]
# df['right_time']=df['time']
# df['right_transaction_type']=df['transaction_type']
# df['right_new_uom']=df['new_uom']
# df['right_rack_id']=df['rack_id']
# df['right_rack_face']=df['rack_face']
# df['right_slot']=df['slot']
# df.drop(columns=['time','trasaction_type','new_uom','rack_id','rack_face','slot'])
# df_inner=pd.merge(df,df_case1,how='inner')
# df_case3=pd.merge(df_case1,df,how='left')
# df_case2=pd.merge(df_case1,df,how='right')
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
# df=pd.merge(df_case11,df_case12,how='outer')
# df2=pd.merge(df,df_case2,how='outer')
# df=pd.merge(df2,df_case3,how='outer')
# cursor = connection.cursor()
# cursor.execute("select * from inventory_transaction_archives order by time desc limit 1")
# Orders = cursor.fetchall()
# df_time = pd.DataFrame(Orders,columns=['ID','transaction_id','external_service_request_id','transaction_type','request_id','event_name','user_name','slot','tpid','item_id','old_uom','delta_uom','new_uom','process_id','time','pps_id','ppsbin_id'])
# latest=df_time.time #giving NaN to rest after 1st row
# df['latest_inventory_transaction'] = latest
# def check_if_null(a, b):
#         if a.isna:
#             val= b
#         else:
#             val= a
#         return int(val)

# df.transaction_time= check_if_null(df.transaction_time,df.first_put_time)
# df['untouched_ageing']=pd.Timedelta(df.transaction_time - df.latest_inventory_transaction).seconds / 3600.0
# df['slot_ageing']=pd.Timedelta(df.first_put_time - df.latest_inventory_transaction).seconds / 3600.0
# #select rack_id and rack_type from data_rack and join on rack_id to find rack_type
# df['cron_ran_at']=datetime.now()