# import sys
#
# sys.path.insert(0, '/Users/srijan.c/GO_Python_Scripts/Modules')
# import psycopg2
# from ConnectionAndQuery import ConnectionToInflux, ConnectionToPostgres
# from GOMail import send_mail
# from datetime import datetime, timedelta, date, timezone
# from GOTimeStamps import get_time_interval
# import time
# import pandas as pd
# from influxdb import InfluxDBClient, DataFrameClient
# import numpy as np
# from datetime import datetime, timedelta
# import warnings
#
# warnings.filterwarnings("ignore")
#
# # configurations
#
# # adidas
# ip = '10.11.4.23'
# port = '8086'
# site = 'Adidas'
# srms_ip = '172.19.40.47'
# srms_port = '5435'
# timez = 'PDT'
#
# # p1
# # ip='10.11.4.23'
# # port='8086'
# # site = 'p1'
# # srms_ip = '10.70.133.76'
# # srms_port = '5432'
# # timez = 'CST'
#
# # make it true for p1
# streaming = False
#
# # required connections
# con_error = {'Messaage': []}
# conn = ConnectionToInflux(site)
# client_tuple = conn.makeConnection(ip, port, 'gor', 'Grey()orange', 'GreyOrange')
# con_error['Messaage'].append(client_tuple[2]) if client_tuple[0] == 0 else print('')
#
# # ---------------------------------------------------------------------------------------------------------------------------
#
# sd1 = '2022-11-09 00:00:00'
# sd2 = '2022-11-04 00:00:00'
# ed = '2022-11-10 00:00:00'
#
# qry1 = "select * from order_events where event_name='order_complete' and time>='" + sd1 + "' and time<'" + ed + "' order by time desc"
# qry2 = "select * from order_events where time>='" + sd2 + "' and time<'" + ed + "' order by time desc"
# qry3 = "select order_id,pps_id,installation_id from streaming_order_events where time>='" + sd2 + "' and time<'" + ed + "' order by time desc"
#
# # fetching data
#
# order_events_completed = ConnectionToInflux.fetch_data(client_tuple, qry1)
# order_events_completed = pd.DataFrame(order_events_completed)
# order_events_completed['time'] = pd.to_datetime(order_events_completed['time'])
# order_events_completed['time'] = order_events_completed['time'].apply(lambda x: x.to_pydatetime())
#
# # fetch data in batches
# start_date = pd.date_range(sd2, ed, freq='30min', closed='left')
# end_date = pd.date_range(sd2, ed, freq='30min', closed='right')
# df = start_date.to_frame(index=False)
# df["interval_end"] = end_date.to_frame(index=False)
# df.rename(columns={0: 'interval_start'}, inplace=True)
# df['qry'] = df.apply(
#     lambda x: "select * from order_events where time>='" + str(x['interval_start']) + "' and time<'" + str(
#         x['interval_end']) + "' order by time desc", axis=1)
#
# ideal_dataset = pd.DataFrame(columns=['time', 'bin_id', 'bintags', 'breach_state', 'breached', 'critical',
#                                       'event_name', 'external_service_request_id', 'host', 'installation_id',
#                                       'item_id', 'order_flow_name', 'order_id', 'pick_after_time',
#                                       'pick_before_time', 'picked_quantity', 'pps_id', 'ppsbin_id',
#                                       'seat_name', 'tag_order_flow_name', 'value'])
#
# for i in range(len(df)):
#     order_events_all5days = ConnectionToInflux.fetch_data(client_tuple, df['qry'][i])
#     if 'error' in order_events_all5days[0].keys():
#         pass
#     else:
#         order_events_all5days = pd.DataFrame(order_events_all5days)
#         ideal_dataset = ideal_dataset.append(order_events_all5days, ignore_index=False)
#
# order_events_all5days = ideal_dataset
# order_events_all5days['time'] = pd.to_datetime(order_events_all5days['time'])
# order_events_all5days['time'] = order_events_all5days['time'].apply(lambda x: x.to_pydatetime())
#
# replacestrings = {None: '2001-01-01T01:01:01Z', 'undefined': '2001-01-01T01:01:01Z'}
# order_events_all5days["pick_before_time"].replace(replacestrings, inplace=True)
# order_events_all5days["pick_after_time"].replace(replacestrings, inplace=True)
#
# order_events_all5days['pick_before_time'] = pd.to_datetime(order_events_all5days['pick_before_time'])
# order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'].apply(lambda x: x.to_pydatetime())
# order_events_all5days['pick_after_time'] = pd.to_datetime(order_events_all5days['pick_after_time'])
# order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'].apply(lambda x: x.to_pydatetime())
#
# if streaming == True:
#     streaming_orders = ConnectionToInflux.fetch_data(client_tuple, qry3)
#     streaming_orders = pd.DataFrame(streaming_orders)
#     streaming_orders['time'] = pd.to_datetime(streaming_orders['time'])
#     streaming_orders['time'] = streaming_orders['time'].apply(lambda x: x.to_pydatetime())
#     streaming_orders['breach_state'] = 'non_breach'
#     streaming_orders['critical'] = 'false'
#     streaming_orders['event_name'] = 'order_created'
# else:
#     streaming_orders = pd.DataFrame(
#         columns=['time', 'order_id', 'pps_id', 'installation_id', 'breached_state', 'critical', 'event_name'])
#
# order_events_completed = pd.DataFrame(
#     order_events_completed.groupby('order_id')['time'].count().reset_index().reset_index())
# order_events_completed.rename(columns={'index': 'group'}, inplace=True)
# order_events_completed.drop(['time'], axis=1, inplace=True)
#
# order_events_all5days = pd.merge(order_events_all5days, order_events_completed, on='order_id')
#
# streaming_all5days = pd.merge(streaming_orders, order_events_all5days[['order_id']], on='order_id')
# streaming_all5days.sort_values('time', ascending=True, inplace=True)
# streaming_all5days.drop_duplicates(inplace=True)
#
# order_events_all5days = pd.concat([streaming_all5days, order_events_all5days])
# order_events_all5days = pd.merge(order_events_all5days, order_events_completed, on='order_id')
# order_events_all5days.drop(['group_x', 'tag_order_flow_name'], axis=1, inplace=True)
# order_events_all5days.rename(columns={'group_y': 'group'}, inplace=True)
#
# di = {'order_created': 1, 'order_tasked': 2, 'item_picked': 3, 'order_complete': 4, 'order_temporary_unfulfillable': 5,
#       'inventory_awaited': 6, 'order_recalculated_for_deadlock': 7, 'temporary_unfulfillable': 8,
#       'order_unfulfillable': 9}
# order_events_all5days["event_name_unique"] = order_events_all5days["event_name"]
# order_events_all5days["event_name_unique"].replace(di, inplace=True)
#
# order_events_all5days = order_events_all5days[
#     ~((order_events_all5days['picked_quantity'] == '0') & (order_events_all5days['event_name'] == "item_picked"))]
# order_events_all5days.sort_values(by=['order_id', 'time'], ascending=True, inplace=True)
# dataset1 = order_events_all5days.copy()
#
# order_events_all5days['order_created'] = order_events_all5days.apply(
#     lambda x: x['time'] if x['event_name_unique'] == 1 else np.nan, axis=1)
# order_events_all5days['order_tasked'] = order_events_all5days.apply(
#     lambda x: x['time'] if x['event_name_unique'] == 2 else np.nan, axis=1)
# order_events_all5days['item_picked'] = order_events_all5days.apply(
#     lambda x: x['time'] if x['event_name_unique'] == 3 else np.nan, axis=1)
# order_events_all5days['order_complete'] = order_events_all5days.apply(
#     lambda x: x['time'] if x['event_name_unique'] == 4 else np.nan, axis=1)
# order_events_all5days['PBT'] = order_events_all5days['pick_before_time']
# order_events_all5days['breached'] = order_events_all5days.apply(
#     lambda x: 'false' if x['event_name'] == 'order_complete' and x['PBT'] > x['order_complete'] else (
#         'true' if x['event_name'] == 'order_complete' and x['PBT'] < x['order_complete'] else np.nan), axis=1)
# dataset2 = order_events_all5days.copy()
#
# order_events_all5days['picked_quantity'] = order_events_all5days['picked_quantity'].replace(np.nan, 0)
# order_events_all5days['picked_quantity'] = order_events_all5days['picked_quantity'].astype('int64')
#
# order_events_all5days['bin_id'] = order_events_all5days['bin_id'].replace(np.nan, 0)
# order_events_all5days['bin_id'] = order_events_all5days['bin_id'].astype('int64')
#
# df1 = order_events_all5days.groupby(
#     ['installation_id', 'order_id', 'group', 'event_name_unique', 'event_name', 'bin_id']).agg(
#     {'order_created': 'last', 'order_tasked': 'last', 'item_picked': 'first', 'order_complete': 'last', 'PBT': 'max',
#      'picked_quantity': 'sum', 'breached': 'last'}).reset_index()
# df2 = pd.DataFrame(order_events_all5days.groupby(
#     ['installation_id', 'order_id', 'group', 'event_name_unique', 'event_name', 'bin_id', 'pps_id'])['pps_id'].count())
# df2.rename(columns={'pps_id': 'count'}, inplace=True)
# df2 = df2.reset_index()
# df2 = df2[df2['event_name'] == 'item_picked']
# df2 = df2[['installation_id', 'order_id', 'group', 'pps_id']]
#
# order_events_all5days = pd.merge(df1, df2, on=['installation_id', 'order_id', 'group'])
#
# streamingpart = dataset2.groupby(['installation_id', 'order_id', 'group', 'event_name']).agg(
#     {'order_created': 'last'}).reset_index()
# streamingpart = streamingpart.pivot(index=['installation_id', 'order_id', 'group'], columns=['event_name'],
#                                     values=['order_created'])
# streamingpart = streamingpart['order_created'].reset_index()
# streamingpart = streamingpart[streamingpart['order_created'].isna()]
# streamingpart['event_name'] = 'order_created'
# streamingpart['event_name_unique'] = 1
# streamingpart['order_created'] = sd2
#
# order_events_all5days = pd.concat([streamingpart, order_events_all5days], ignore_index=True)
# order_events_all5days = order_events_all5days[
#     ['installation_id', 'order_id', 'group', 'event_name_unique', 'event_name', 'order_created', 'item_picked',
#      'order_complete', 'order_tasked', 'PBT', 'picked_quantity', 'pps_id', 'bin_id', 'breached']]
#
# replacestrings = {None: '2001-01-01 01:01:01', 'undefined': '2001-01-01 01:01:01'}
# order_events_all5days["order_created"].replace(replacestrings, inplace=True)
# order_events_all5days['order_created'] = pd.to_datetime(order_events_all5days['order_created'], utc=True)
# order_events_all5days['breached'] = order_events_all5days.groupby('group')['breached'].shift(-3, axis=0)
#
# otu = order_events_all5days[order_events_all5days['event_name'] == 'order_temporary_unfulfillable']
# otu = otu[['installation_id', 'order_id', 'group', 'event_name']]
# otu['order_temporary_unfulfillable'] = 'true'
#
# oia = order_events_all5days[order_events_all5days['event_name'] == 'order_inventory_awaited']
# oia = oia[['installation_id', 'order_id', 'group', 'event_name']]
# oia['order_inventory_awaited'] = 'true'
#
# orfd = order_events_all5days[order_events_all5days['event_name'] == 'order_recalculated_for_deadlock']
# orfd = oia[['installation_id', 'order_id', 'group', 'event_name']]
# orfd['order_recalculated_for_deadlock'] = 'true'
#
# tu = order_events_all5days[order_events_all5days['event_name'] == 'temporary_unfulfillable']
# tu = oia[['installation_id', 'order_id', 'group', 'event_name']]
# tu['temporary_unfulfillable'] = 'true'
#
# ou = order_events_all5days[order_events_all5days['event_name'] == 'order_unfulfillable']
# ou = oia[['installation_id', 'order_id', 'group', 'event_name']]
# ou['order_unfulfillable'] = 'true'
#
# order_events_all5days = pd.merge(order_events_all5days, otu, on=['installation_id', 'order_id', 'group', 'event_name'],
#                                  how='left')
# order_events_all5days['order_temporary_unfulfillable'] = order_events_all5days['order_temporary_unfulfillable'].fillna(
#     'false')
#
# order_events_all5days = pd.merge(order_events_all5days, oia, on=['installation_id', 'order_id', 'group', 'event_name'],
#                                  how='left')
# order_events_all5days['order_inventory_awaited'] = order_events_all5days['order_inventory_awaited'].fillna('false')
#
# order_events_all5days = pd.merge(order_events_all5days, orfd, on=['installation_id', 'order_id', 'group', 'event_name'],
#                                  how='left')
# order_events_all5days['order_recalculated_for_deadlock'] = order_events_all5days[
#     'order_recalculated_for_deadlock'].fillna('false')
#
# order_events_all5days = pd.merge(order_events_all5days, tu, on=['installation_id', 'order_id', 'group', 'event_name'],
#                                  how='left')
# order_events_all5days['temporary_unfulfillable'] = order_events_all5days['temporary_unfulfillable'].fillna('false')
#
# order_events_all5days = pd.merge(order_events_all5days, ou, on=['installation_id', 'order_id', 'group', 'event_name'],
#                                  how='left')
# order_events_all5days['order_unfulfillable'] = order_events_all5days['order_unfulfillable'].fillna('false')
#
# order_events_all5days['picked_quantity'] = order_events_all5days.groupby('group')['picked_quantity'].shift(-2, axis=0)
#
# order_events_all5days['order_tasked'] = order_events_all5days.groupby('group')['order_tasked'].shift(-1, axis=0)
# order_events_all5days['item_picked'] = order_events_all5days.groupby('group')['item_picked'].shift(-2, axis=0)
#
# order_events_all5days['order_complete'] = order_events_all5days.groupby('group')['order_complete'].shift(-3, axis=0)
# order_events_all5days['bin_id'] = order_events_all5days.groupby('group')['bin_id'].shift(-3, axis=0)
#
# order_events_all5days['PBT'] = order_events_all5days['PBT'].astype('object')
#
# order_events_all5days['pbt1'] = order_events_all5days.groupby('group')['PBT'].shift(-3, axis=0)
# order_events_all5days['pbt2'] = order_events_all5days.groupby('group')['PBT'].shift(-2, axis=0)
# order_events_all5days['pbt3'] = order_events_all5days.groupby('group')['PBT'].shift(-1, axis=0)
#
# order_events_all5days['pbt1'] = order_events_all5days['pbt1'].astype('object')
# order_events_all5days['pbt2'] = order_events_all5days['pbt2'].astype('object')
# order_events_all5days['pbt3'] = order_events_all5days['pbt3'].astype('object')
#
# replacestrings = {None: '2001-01-01 01:01:01', 'undefined': '2001-01-01 01:01:01'}
# order_events_all5days["pbt1"].replace(replacestrings, inplace=True)
# order_events_all5days["pbt2"].replace(replacestrings, inplace=True)
# order_events_all5days["pbt3"].replace(replacestrings, inplace=True)
#
# order_events_all5days['PBT'] = order_events_all5days.apply(
#     lambda x: x['pbt1'] if x['pbt1'] != '2001-01-01 01:01:01' else (
#         x['pbt2'] if x['pbt2'] != '2001-01-01 01:01:01' else x['pbt3']), axis=1)
# order_events_all5days.drop(['pbt1', 'pbt2', 'pbt3'], axis=1, inplace=True)
#
# order_events_all5days = order_events_all5days[order_events_all5days['event_name_unique'] == 1]
#
# replacestrings = {None: '2001-01-01 01:01:01', 'undefined': '2001-01-01 01:01:01'}
# order_events_all5days["PBT"].replace(replacestrings, inplace=True)
# order_events_all5days['PBT'] = pd.to_datetime(order_events_all5days['PBT'], utc=True)
#
# order_events_all5days['creation_to_tasked'] = (
#             order_events_all5days['order_tasked'] - order_events_all5days['order_created'])
# order_events_all5days['tasked_to_first_item_picked'] = (
#             order_events_all5days['item_picked'] - order_events_all5days['order_tasked'])
# order_events_all5days['first_item_picked_to_order_complete'] = (
#             order_events_all5days['order_complete'] - order_events_all5days['item_picked'])
# order_events_all5days['time_to_pick'] = (order_events_all5days['PBT'] - order_events_all5days['order_created'])
# order_events_all5days['pbt_to_completion'] = (order_events_all5days['PBT'] - order_events_all5days['order_complete'])
# order_events_all5days['pbt_to_task'] = (order_events_all5days['PBT'] - order_events_all5days['order_tasked'])
# order_events_all5days['created_to_completed'] = (
#             order_events_all5days['order_complete'] - order_events_all5days['order_created'])
#
# order_events_all5days['creation_to_tasked'] = order_events_all5days['creation_to_tasked'].apply(
#     lambda x: x.total_seconds())
# order_events_all5days['tasked_to_first_item_picked'] = order_events_all5days['tasked_to_first_item_picked'].apply(
#     lambda x: x.total_seconds())
# order_events_all5days['first_item_picked_to_order_complete'] = order_events_all5days[
#     'first_item_picked_to_order_complete'].apply(lambda x: x.total_seconds())
# order_events_all5days['time_to_pick'] = order_events_all5days['time_to_pick'].apply(lambda x: x.total_seconds())
# order_events_all5days['pbt_to_completion'] = order_events_all5days['pbt_to_completion'].apply(
#     lambda x: x.total_seconds())
# order_events_all5days['pbt_to_creation'] = order_events_all5days['time_to_pick']
# order_events_all5days['pbt_to_task'] = order_events_all5days['pbt_to_task'].apply(lambda x: x.total_seconds())
# order_events_all5days['created_to_completed'] = order_events_all5days['created_to_completed'].apply(
#     lambda x: x.total_seconds())
#
# order_events_all5days['total_time'] = order_events_all5days['creation_to_tasked'] + order_events_all5days[
#     'tasked_to_first_item_picked'] + order_events_all5days['first_item_picked_to_order_complete']
# order_events_all5days['order_temporary_unfulfillable'] = order_events_all5days.apply(
#     lambda x: x['temporary_unfulfillable'] if x['temporary_unfulfillable'] == 'true' else x[
#         'order_temporary_unfulfillable'], axis=1)
# order_events_all5days.drop(['temporary_unfulfillable'], axis=1, inplace=True)
#
# dataset2.sort_values(by="time", ascending=False, inplace=True)
# add_pbt_and_pat = dataset2[dataset2['event_name_unique'] == 4]
# add_pbt_and_pat = add_pbt_and_pat.groupby('order_id').agg(
#     {'pick_before_time': 'first', 'pick_after_time': 'first', 'order_flow_name': 'first', 'time': 'first',
#      'bintags': 'first'}).reset_index()
#
# breached_order_metrics = dataset2[
#     (dataset2['event_name_unique'] == 4) | (dataset2['event_name_unique'] == 1) | (dataset2['event_name_unique'] == 2)]
# breached_order_metrics = breached_order_metrics.groupby(
#     ['installation_id', 'order_id', 'group', 'event_name', 'event_name_unique']).agg(
#     {'breach_state': 'last'}).reset_index()
# breached_order_metrics = breached_order_metrics[['order_id', 'event_name', 'breach_state']]
# breached_order_metrics = breached_order_metrics.pivot(index=['order_id'], columns=['event_name'],
#                                                       values=['breach_state'])
# breached_order_metrics = breached_order_metrics.add_prefix('breached_')
# breached_order_metrics = breached_order_metrics['breached_breach_state'].reset_index()
#
# breached_order_metrics2 = dataset1[(dataset1['event_name_unique'] == 4) | (dataset1['event_name_unique'] == 2)]
# breached_order_metrics2 = breached_order_metrics2.groupby(
#     ['installation_id', 'order_id', 'group', 'event_name', 'event_name_unique']).agg({'breached': 'last'}).reset_index()
# breached_order_metrics2 = breached_order_metrics2[['order_id', 'event_name', 'breached']]
# breached_order_metrics2 = breached_order_metrics2.pivot(index=['order_id'], columns=['event_name'], values=['breached'])
# breached_order_metrics2 = breached_order_metrics2['breached'].reset_index()
# dic = {'order_complete': 'breached_completed',
#        'order_tasked': 'breached_tasked'}
#
# breached_order_metrics2.rename(columns=dic, inplace=True)
# breached_order_info = pd.merge(breached_order_metrics, breached_order_metrics2, on='order_id')
# breached_order_info = pd.merge(breached_order_info, add_pbt_and_pat, on='order_id')
# order_events_all5days = pd.merge(order_events_all5days, breached_order_info, on='order_id')
#
# order_events_all5days.drop(['event_name', 'event_name_unique', 'group', 'item_picked', 'order_complete'], axis=1,
#                            inplace=True)
#
# dic = {'order_created': 'order_created_time',
#        'order_tasked': 'order_tasked_time'}
#
# order_events_all5days.rename(columns=dic, inplace=True)
#
# ext_req_id = pd.DataFrame(order_events_all5days.groupby('order_id')['installation_id'].count()).reset_index()
# ext_req_id = ext_req_id[['order_id']]
# ext_req_id['order_id'] = ext_req_id['order_id'].apply(lambda x: 'id=' + str(x))
# ext_req_id['contains_group'] = ext_req_id['order_id'].apply(lambda x: 1 if 'group' in x else 0)
# ext_req_id_no_gr = ext_req_id[ext_req_id['contains_group'] != 1]
# countofrec = len(ext_req_id_no_gr['order_id'])
#
# concat_string = ' or '.join(list(ext_req_id_no_gr['order_id']))
# if countofrec == 0:
#     data_ext_req_id = pd.DataFrame(columns=['order_id', 'external_service_request_id'])
#     data_ext_req_id['order_id'] = data_ext_req_id['order_id'].astype('object')
# else:
#     qry = "select id,external_service_request_id   from service_request where " + concat_string
#     conn_pg = ConnectionToPostgres(site)
#     client_srms = conn_pg.makeConnection(srms_ip, srms_port, 'platform_srms', 'altryx_read', 'YLafmbfuji$@#45!', None,
#                                          None, None)
#     data_ext_req_id = ConnectionToPostgres.fetch_data(client_srms, qry)
#     data_ext_req_id = pd.DataFrame(data_ext_req_id, columns=['order_id', 'external_service_request_id'])
#     data_ext_req_id['order_id'] = data_ext_req_id['order_id'].astype('str')
#
# order_events_all5days['order_id'] = order_events_all5days['order_id'].astype('str')
# order_events_all5days = pd.merge(order_events_all5days, data_ext_req_id, on='order_id', how='left')
#
# order_events_all5days['external_service_request_id'] = order_events_all5days['external_service_request_id'].fillna('NA')
# if timez == 'JST':
#     h = 9
#     m = 0
# elif timez == 'EDT':
#     h = -5
#     m = 0
# elif timez == 'EST':
#     h = -4
#     m = 0
# elif timez == 'IST':
#     h = 5
#     m = 30
# elif timez == 'ACT':
#     h = 10
#     m = 0
# elif timez == 'CET':
#     h = 2
#     m = 0
# elif timez == 'PDT':
#     h = -8
#     m = 0
# elif timez == 'PST':
#     h = -7
#     m = 0
# elif timez == 'AST':
#     h = -4
#     m = 0
# elif timez == 'CST':
#     h = -3
#     m = 0
# else:
#     h = 0
#     m = 0
#
# order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'] + timedelta(hours=h)
# order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'] + timedelta(minutes=m)
#
# order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'] + timedelta(hours=h)
# order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'] + timedelta(minutes=m)
# order_events_all5days['pps_id'] = order_events_all5days['pps_id'].fillna('0')
#
# order_events_all5days['order_created_time'] = order_events_all5days['order_created_time'].astype('object')
# order_events_all5days['order_tasked_time'] = order_events_all5days['order_tasked_time'].astype('object')
# order_events_all5days['PBT'] = order_events_all5days['PBT'].astype('object')
# order_events_all5days['pick_before_time'] = order_events_all5days['pick_before_time'].astype('object')
# order_events_all5days['pick_after_time'] = order_events_all5days['pick_after_time'].astype('object')
# order_events_all5days['bin_id'] = order_events_all5days['bin_id'].astype('object')
# order_events_all5days['pps_id'] = order_events_all5days['pps_id'].astype('int')
#
# order_events_all5days.rename(columns={'picked_quantity': 'total_picked_quantity'}, inplace=True)
#
# print(order_events_all5days)