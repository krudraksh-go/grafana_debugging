## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction
from pandasql import sqldf
import numpy as np
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
db_name =os.environ.get('Out_db_name', 'airflow')

#
# dag = DAG(
#     'slot_utilzation',
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
class slot_utilzation:
    def divide_slotref(self, df):
        df['rack'] = df['slot_id'].str.split('.', expand=True)[0]
        df['face'] = df['slot_id'].str.split('.', expand=True)[1]
        df['slot'] = df['slot_id'].str.split('.', 2, expand=True)[2]
        return df

    def slot_utilzation_calc(self, tenant_info, end_date, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        # q = f"select * from slots_ageing_ayx_latest"
        # df = pd.DataFrame(self.client.query(q).get_points())
        table_name = f'{rp_seven_days}.slots_ageing_elk'

        q = f"select * from {table_name} where time > now()-1d and host = '{self.tenant_info['host_name']}' order by time desc limit 1"
        cron_run_at = pd.DataFrame(self.client.query(q).get_points())
        if cron_run_at.empty:
            q = f"select * from {table_name} where time > now()-7d  order by time desc limit 1"
            cron_run_at = pd.DataFrame(self.client.query(q).get_points())

        start_date = cron_run_at["cron_ran_at"][0]

        q_cnt = f"select count(cron_ran_at) from {table_name} where time>'{start_date}' and host = '{self.tenant_info['host_name']}' order by time desc"

        temp_slotdata = pd.DataFrame(self.client.query(q_cnt).get_points())
        q_cnt = temp_slotdata['count'][0]
        offset_val = 0
        data_limit = 5000
        while offset_val < q_cnt:
            q = f"select * from {table_name} where time>'{start_date}' and host = '{self.tenant_info['host_name']}' order by time desc limit {data_limit} offset {offset_val}"
            temp_slotdata = pd.DataFrame(self.client.query(q).get_points())
            if offset_val == 0:
                df = temp_slotdata
            else:
                df = pd.concat([df, temp_slotdata])
            offset_val = offset_val + data_limit
        df = df.reset_index()
        if 'level_0' in df:
             del df['level_0']

        self.end_date= end_date

        if not df.empty:
            df = self.divide_slotref(df)
            cron_run_at_date_final=df['cron_ran_at'][0]
            df['unique_id'] = df['item_id'].astype(str) + '_' + df['rack'].astype(str)  + '_' + df['face'].astype(str)
            df['key'] = 1
            racktype_df = df.copy()
            # df=df.groupby(['unique_id','racktype']).agg(count=('key', 'sum'), )
            racktype_df = racktype_df.groupby(['unique_id', 'racktype']).agg(count=('key', 'sum'),
                                                                             time=('time', 'max'), )
            racktype_df = racktype_df.reset_index()
            # df['slots_with_same_sku_per_rack_face']=df.groupby('racktype', as_index=False)['count'].mean()
            racktype_df = racktype_df.groupby('racktype', as_index=False).agg(
                slots_with_same_sku_per_rack_face=('count', 'mean'), time=('time', 'max'), )
            racktype_df = racktype_df.reset_index()
            # df['count']=df.groupby(['unique_id']).key.sum()
            # df2=pd.DataFrame()
            # df2['slots_with_same_sku_per_rack_face']=df.groupby('unique_id', as_index=False)['count'].mean()
            # df2['racktype']='ALL'
            # df1=pd.merge(df,df2,how='outer')
            all_df = df.copy()
            all_df = all_df.groupby(['unique_id']).agg(count=('key', 'sum'), time=('time', 'max'), )
            all_df = all_df.reset_index()
            all_df['racktype'] = 'ALL'
            all_df = all_df.groupby('racktype', as_index=False).agg(slots_with_same_sku_per_rack_face=('count', 'mean'),
                                                                    time=('time', 'max'), )
            df1 = all_df.append(racktype_df)
            df1 = df1.reset_index()
            if 'index' in df1.columns:
                del df1['index']

            # 2
            racktype_df = df.copy()
            racktype_df = racktype_df.groupby("racktype").agg({"item_id": pd.Series.nunique})
            racktype_df = racktype_df.reset_index()
            all_df = pd.DataFrame()
            all_df['item_id'] = df.agg({"item_id": pd.Series.nunique})
            all_df['racktype'] = 'ALL'
            df2 = all_df.append(racktype_df)
            df2 = df2.reset_index()
            if 'index' in df2.columns:
                del df2['index']

            df2 = df2.rename(columns={'item_id': 'distinct_sku'})
            # 3
            q = f"select cron_ran_at from racktype_info_ayx where time <= '{self.end_date}' order by time desc limit 1"
            cron_run_at = pd.DataFrame(self.client.query(q).get_points())
            cron_run_at_date = cron_run_at["cron_ran_at"][0]
            q = f"select * from racktype_info_ayx where cron_ran_at='{cron_run_at_date}'"
            dfa = pd.DataFrame(self.client.query(q).get_points())
            dfb = pd.DataFrame()
            dfa['consolidated_total_rack'] = dfa['total_racks'] * dfa['no_of_rack_faces']
            dfa['key'] = 'ALL'
            dfb = dfa.groupby('key', as_index=False).agg(consolidated_total_rack=('consolidated_total_rack', 'sum'),
                                                         total_slots=('total_slots', 'sum'), )
            dfb = dfb.reset_index()
            # dfb['total_slots']=dfa.sum('total_slots')
            # dfb['sum_consolidated_total_rack']=df.sum('consolidated_total_rack')
            # df['rack_id'] = df['slot'].str.split('.', expand=True)[0]
            # df['rack_face'] = df['slot'].str.split('.', expand=True)[1]
            # df['slot'] = df['slot'].str.split('.', expand=True)[2]
            # slot_df = df.groupby(["rack","face"]).agg({"slot": pd.Series.nunique})
            slot_df = df.groupby(['rack', 'face']).agg(slots_with_inv=('slot', 'nunique'), )
            slot_df = slot_df.reset_index()

            dfb['slots_with_inv'] = slot_df['slots_with_inv'].sum()
            dfb['count_of_slots'] = slot_df['slots_with_inv'].count()
            dfb['count_face_with_0_inv'] = dfb['consolidated_total_rack'] - dfb['count_of_slots']
            del dfb['count_of_slots']
            dfb = dfb.rename(columns={'key': 'racktype', 'consolidated_total_rack': 'total_racks'})
            df3 = dfb
            if 'index' in df3.columns:
                del df3['index']

            # 4

            dfa = df.copy()
            dfa = dfa.groupby(["racktype", "rack", "face"]).agg(slots_with_inv=('slot', 'nunique'), )
            dfa = dfa.reset_index()
            dfa = dfa.groupby(["racktype", "rack"]).agg(slots_with_inv=('slots_with_inv', 'sum'), )
            # dfa['racktype']='ALL'
            q = f"select * from racktype_info_ayx where cron_ran_at='{cron_run_at_date}'"
            df_racktype = pd.DataFrame(self.client.query(q).get_points())
            dfb = pd.merge(dfa, df_racktype, how='inner', on='racktype')
            dfb['rack_face_utilization'] = dfb['slots_with_inv'] / dfb['total']
            dfb['rack_face_utilization'] = dfb['rack_face_utilization'] * 100
            dfb['consolidated_total_rack'] = dfb['total_racks'] * dfb['no_of_rack_faces']
            dfb = dfb.groupby('racktype').agg(slots_with_inv=('slots_with_inv', 'sum'),
                                              count_of_slots=('slots_with_inv', 'count'),
                                              rack_face_utilization=('rack_face_utilization', 'sum'),
                                              total_slots=('total_slots', 'first'), \
                                              first_no_of_rackface=('no_of_rack_faces', 'first'),
                                              first_consolidated_total_rack=('consolidated_total_rack', 'first'), )
            # dfb['slots_with_inv']=dfb.groupby('racktype').sum('slots_with_inv')
            # dfb['count_of_slots']=dfb.groupby('racktype').count('slots_with_inv')
            # dfb['sum_rack_face_utilization']=dfb.groupby('racktype').sum('rack_face_utilization')
            # dfb['total_slots']=dfb.groupby('racktype').first('total_slots')
            # dfb['first_no_of_rackface']=dfb.groupby('racktype').first('no_of_rackface')
            # dfb['first_consolidated_total_rack']=dfb.groupby('racktype').first('consolidated_total_rack')
            dfb = dfb.reset_index()
            dfb['avg_rack_face_utilization'] = dfb['rack_face_utilization'] / dfb['first_consolidated_total_rack']
            dfb['count_face_with_0_inv'] = dfb['first_consolidated_total_rack'] - dfb['count_of_slots']
            dfb = dfb.rename(columns={"first_consolidated_total_rack": "total_racks"})
            dfb['percentage_slots_with_inv'] = dfb['slots_with_inv'] / dfb['total_slots']
            dfb['percentage_slots_with_inv'] = dfb['percentage_slots_with_inv'] * 100.0
            del dfb['count_of_slots']
            del dfb['first_no_of_rackface']
            # del dfb['rack_face_utilization']
            df4 = dfb
            # 5
            # dfc=pd.DataFrame(client.query(q).get_points())
            # dfc['rack_face_utilization']=dfb['rack_face_utilization']
            # dfc['avg_rack_face_utilization']=dfb['rack_face_utilization']/dfb['first_consolidated_total_rack']
            # dfc['racktype']='ALL'
            # df4=pd.merge(dfb,dfc,how='outer')
            df3['avg_rack_face_utilization'] = dfb['rack_face_utilization'].sum()
            df3['avg_rack_face_utilization'] = df3['avg_rack_face_utilization'] / df3['total_racks']
            df3['percentage_slots_with_inv'] = df3['slots_with_inv'] / df3['total_slots']
            df3['percentage_slots_with_inv'] = df3['percentage_slots_with_inv'] * 100.0
            del df4['rack_face_utilization']
            # append df1,df2,df3,df4 with cron_ran_at

            df3 = df3.append(df4)
            final_df = pd.merge(df1, df2, on=['racktype'], how='outer')
            final_df = pd.merge(final_df, df3, on=['racktype'], how='outer')
            final_df['avg_rack_face_utilization'] = round(final_df['avg_rack_face_utilization'], 2)
            final_df['percentage_slots_with_inv'] = round(final_df['percentage_slots_with_inv'], 2)
            final_df['slots_with_same_sku_per_rack_face'] = round(final_df['slots_with_same_sku_per_rack_face'], 2)
            #        final_df.to_csv(r'/home/ankush.j/Desktop/workname/big_data/final_df.csv', index=False, header=True)
            final_df.time = pd.to_datetime(final_df.time)
            final_df = final_df.set_index('time')
            final_df['record_id'] = np.arange(1, (final_df.shape[0] + 1), 1)
            final_df['cron_ran_at'] = cron_run_at_date_final
            final_df['avg_rack_face_utilization'] = final_df['avg_rack_face_utilization'].astype(float)
            final_df['count_face_with_0_inv'] = final_df['count_face_with_0_inv'].astype(float)
            final_df['distinct_sku'] = final_df['distinct_sku'].astype(float)
            final_df['percentage_slots_with_inv'] = final_df['percentage_slots_with_inv'].astype(float)
            final_df['slots_with_inv'] = final_df['slots_with_inv'].astype(float)
            final_df['slots_with_same_sku_per_rack_face'] = final_df['slots_with_same_sku_per_rack_face'].astype(float)
            final_df['total_racks'] = final_df['total_racks'].astype(float)
            final_df['total_slots'] = final_df['total_slots'].astype(float)
            final_df['host'] = self.tenant_info['host_name']
            final_df['installation_id'] = self.tenant_info['installation_id']
            final_df['host'] = final_df['host'].astype(str)
            final_df['installation_id'] = final_df['installation_id'].astype(str)
            if 'level_0' in final_df:
                del final_df['level_0']
            self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
            self.write_client.writepoints(final_df, "slots_utilization_ayx_elk", db_name=self.tenant_info["out_db_name"],
                                          tag_columns=['record_id'], dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

        return
    def slot_utilzation_final(self, tenant_info, **kwargs ):
        self.tenant_info = tenant_info['tenant_info']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"],port=self.tenant_info["write_influx_port"],db=self.tenant_info["out_db_name"])

        #self.start_date = self.client.get_start_date("slots_utilization_ayx")
        self.end_date = datetime.now(timezone.utc)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        #self.end_date = self.end_date.replace(minute=0,second=0)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"],port=self.tenant_info["influx_port"],db="GreyOrange")
        self.slot_utilzation_calc(tenant_info, self.end_date)
        return None


with DAG(
    'slot_utilzation_elk',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'calculation of puts per rack face GM-44025',
    schedule_interval = timedelta(hours=1),
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
            if tenant['Active'] == "Y" and tenant['slot_utilzation'] == "Y":
                final_task = PythonOperator(
                    task_id='slot_utilzation_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(slot_utilzation().slot_utilzation_final,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )

    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='slot_utilzation_final',
            provide_context=True,
            python_callable=functools.partial(slot_utilzation().slot_utilzation_final,tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )
