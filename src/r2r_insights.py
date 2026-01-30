import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
import pandas as pd
import numpy as np
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, postgres_connection


class R2R_insights:

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df

    def final_call(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.client = InfluxData(host=self.tenant_info["write_influx_ip"], port=self.tenant_info["write_influx_port"],
                                 db=self.tenant_info["alteryx_out_db_name"])
        #        client.switch_database(db_name)
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db="GreyOrange")

        isvalid = self.client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.client.get_start_date("r2r_insight", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date1 = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date1 = pd.to_datetime(check_start_date1).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from item_picked where time>'{check_start_date1}' and time<='{check_end_date}' and value > 0 limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.start_date = pd.to_datetime(check_start_date)
            self.end_date = datetime.now(timezone.utc)
            self.final_call_1(self.end_date, **kwargs)
        else:
            try:
                check_start_date = pd.to_datetime(check_start_date, utc=True) - timedelta(minutes=90)
                end_datetime = datetime.now(timezone.utc)
                daterange = self.create_time_series_data2(check_start_date, end_datetime, '1h')
                if daterange.empty:
                    daterange = self.create_time_series_data2(check_start_date, end_datetime, '15min')
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.start_date = daterange['start_date'][i]
                    self.final_call_1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception                     
            except Exception as e:
                print(f"error:{e}")
                raise e

    def final_call_1(self, end_date, **kwargs):
        site_name = self.tenant_info["Name"]
        self.utilfunction = CommonFunction()
        expected_value_df = self.utilfunction.get_sheet_from_airflow_setup_excel('expected_values')
        expected_value_df = expected_value_df[expected_value_df['Name']==site_name].reset_index()
        df = pd.DataFrame(
            columns=['time', 'parameter', 'insight', 'per_pps', 'pps_id', 'actual_value', 'expected_value', 'result'])
        self.start_date = self.start_date.strftime("%Y-%m-%d %H:%M:00")
        self.end_date = self.end_date.strftime("%Y-%m-%d %H:%M:00")
        print(f"self.start_date:{self.start_date},self.end_data:{self.end_date}")
        self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                 sslrootcert=self.tenant_info['sslrootcert'],
                                                 sslcert=self.tenant_info['sslcert'], \
                                                 sslkey=self.tenant_info['sslkey'],
                                                 host=self.tenant_info['Postgres_tower'], \
                                                 port=self.tenant_info['Postgres_tower_port'],
                                                 password=self.tenant_info['Tower_password'],
                                                 dag_name=os.path.basename(__file__),
                                                 site_name=self.tenant_info['Name'])
        q = f"select count(*) as g20 from mle_alert where created>='{self.start_date}' and created<'{self.end_date}'and alert_type='Alert1' and (EXTRACT(EPOCH FROM (resolved_timestamp - created))/60)>20 and resolved='true' "
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        A = pd.DataFrame(data, columns=['g20'])
        self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                 sslrootcert=self.tenant_info['sslrootcert'],
                                                 sslcert=self.tenant_info['sslcert'], \
                                                 sslkey=self.tenant_info['sslkey'],
                                                 host=self.tenant_info['Postgres_tower'], \
                                                 port=self.tenant_info['Postgres_tower_port'],
                                                 password=self.tenant_info['Tower_password'],
                                                 dag_name=os.path.basename(__file__),
                                                 site_name=self.tenant_info['Name'])
        q = f"select count(*) as total from mle_alert WHERE created>='{self.start_date}' and created< '{self.end_date}' and resolved='true'"
        data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
        B = pd.DataFrame(data, columns=['total'])
        self.postgres_conn.close()
        mle_alert = pd.DataFrame(columns=['actual_value'])
        breached_function = lambda x: 'Non-Breached' if x['actual_value'] < x['expected_value'] else 'Breached'
        breached_function_reverse = lambda x: 'Non-Breached' if x['actual_value'] >= x['expected_value'] else 'Breached'
        parameter_func='pick R2R'
        if not A.empty and not B.empty:
            actual_value = str(round(100*A['g20'][0] / B['total'][0],2))+'%'
            mle_alert = mle_alert.append({'actual_value': actual_value}, ignore_index=True)
            mle_alert['expected_value'] = str(expected_value_df['r2r__mle_alerts'][0])
            mle_alert['result'] = mle_alert.apply(breached_function, axis=1)
            mle_alert['parameter'] = parameter_func
            mle_alert['insights'] = 'MLE Alerts > 20 mins'
            mle_alert['per_pps'] = 'NO'
            df = pd.concat([df, mle_alert])

        # Free Bots calculation
        q = f"SELECT  mean(free_bots) As Free_Bots FROM mhs_entity_level_stats WHERE  time>='{self.start_date}' and time < '{self.end_date}'"
        free_bots = pd.DataFrame(self.read_client.query(q).get_points())
        if not free_bots.empty:
            free_bots['expected_value'] = int(expected_value_df['r2r__free_bots'][0])
            free_bots.rename(columns={'Free_Bots': 'actual_value'}, inplace=True)
            free_bots['result'] = free_bots.apply(breached_function_reverse, axis=1)
            # free_bots = free_bots.reset_index()
            free_bots['parameter'] = parameter_func
            free_bots['insights'] = 'Free Bots in Warehouse'
            free_bots['per_pps'] = 'NO'
            free_bots = free_bots.reset_index()
            df = pd.concat([df, free_bots])

        # conjection factor calculation
        q = f"select path_travel_time,idc_time,pps_id from task_cycle_times where time>='{self.start_date}' and time < '{self.end_date}' and path_type = 'storable_to_pps_queue'  "
        conjection_factor = pd.DataFrame(self.read_client.query(q).get_points())
        if not conjection_factor.empty:
            conjection_factor = conjection_factor.groupby(['pps_id']).agg(
                mean_path_travel_time=('path_travel_time', 'mean'), mean_idc_time=('idc_time', 'mean'),
                time=('time', 'last'))
            conjection_factor['actual_value'] = conjection_factor['mean_path_travel_time'] / conjection_factor[
                'mean_idc_time']
            conjection_factor['expected_value'] = int(expected_value_df['r2r__congestion_factor'][0])
            conjection_factor['result'] = conjection_factor.apply(breached_function, axis=1)
            conjection_factor['parameter'] = parameter_func
            conjection_factor['insights'] = 'Congestion Factor'
            conjection_factor['per_pps'] = 'YES'
            conjection_factor = conjection_factor.reset_index()
            conjection_factor.drop(['mean_path_travel_time', 'mean_idc_time'], axis=1, inplace=True)
            df = pd.concat([df, conjection_factor])

        # Bot Stop per hour (Highway)
        q = f"SELECT count(barcode_group) as actual_value FROM butler_nav_incidents WHERE  barcode_group = 'Highway'  and (Is_auto_resolved ='No') and time_difference>10 and time>='{self.start_date}' and time < '{self.end_date}'  "
        Bot_stop_highway = pd.DataFrame(self.read_client.query(q).get_points())
        if not Bot_stop_highway.empty:
            Bot_stop_highway['expected_value'] = int(expected_value_df['r2r__bot_stop_highway'][0])
            Bot_stop_highway['result'] = Bot_stop_highway.apply(breached_function, axis=1)
            Bot_stop_highway['parameter'] = parameter_func
            Bot_stop_highway['insights'] = 'Bot Stops per hour (Highway)'
            Bot_stop_highway['per_pps'] = 'NO'
            df = pd.concat([df, Bot_stop_highway])

        #   pps
        q = f"SELECT count(barcode_group) as actual_value FROM butler_nav_incidents WHERE  barcode_group = 'PPS'  and (Is_auto_resolved ='No') and time_difference>10 and time>='{self.start_date}' and time < '{self.end_date}'  "
        Bot_stop_pps = pd.DataFrame(self.read_client.query(q).get_points())
        if not Bot_stop_pps.empty:
            Bot_stop_pps['expected_value'] = int(expected_value_df['r2r__bot_stop_pps'][0])
            Bot_stop_pps['result'] = Bot_stop_pps.apply(breached_function, axis=1)
            Bot_stop_pps['parameter'] = parameter_func
            Bot_stop_pps['insights'] = 'Bot Stops per hour (PPS)'
            Bot_stop_pps['per_pps'] = 'NO'
            df = pd.concat([df, Bot_stop_pps])

        # aisle
        q = f"SELECT count(barcode_group) as actual_value FROM butler_nav_incidents WHERE  barcode_group = 'Aisle'  and (Is_auto_resolved ='No') and time_difference>10 and time>='{self.start_date}' and time < '{self.end_date}'  "
        Bot_stop_aisle = pd.DataFrame(self.read_client.query(q).get_points())
        if not Bot_stop_aisle.empty:
            Bot_stop_aisle['expected_value'] = int(expected_value_df['r2r__bot_stop_aisle'][0])
            Bot_stop_aisle['result'] = Bot_stop_aisle.apply(breached_function, axis=1)
            Bot_stop_aisle['parameter'] = parameter_func
            Bot_stop_aisle['insights'] = 'Bot Stops per hour (Aisle)'
            Bot_stop_aisle['per_pps'] = 'NO'
            df = pd.concat([df, Bot_stop_aisle])

        # storable
        q = f"SELECT count(barcode_group) as actual_value FROM butler_nav_incidents WHERE  barcode_group = 'storable'  and (Is_auto_resolved ='No') and time_difference>10 and time>='{self.start_date}' and time < '{self.end_date}'  "
        Bot_stop_storable = pd.DataFrame(self.read_client.query(q).get_points())
        if not Bot_stop_storable.empty:
            Bot_stop_storable['expected_value'] = int(expected_value_df['r2r__bot_stop_storable'][0])
            Bot_stop_storable['result'] = Bot_stop_storable.apply(breached_function, axis=1)
            Bot_stop_storable['parameter'] = parameter_func
            Bot_stop_storable['insights'] = 'Bot Stops per hour (Storable)'
            Bot_stop_storable['per_pps'] = 'NO'
            df = pd.concat([df, Bot_stop_storable])

        # light curtain breach
        q = f"SELECT pps_id,time_difference FROM light_curtain_events_airflow WHERE time>='{self.start_date}' and time < '{self.end_date}'  "
        lcb = pd.DataFrame(self.read_client.query(q).get_points())
        if not lcb.empty:
            lcb = lcb.groupby(['pps_id']).agg(actual_value=('time_difference', 'mean'), time=('time', 'last'))
            lcb['expected_value'] = int(expected_value_df['r2r__light_curtain_breach'][0])
            lcb['result'] = lcb.apply(breached_function, axis=1)
            lcb['parameter'] = parameter_func
            lcb['insights'] = 'Light Curtain Breach resolution time'
            lcb['per_pps'] = 'YES'
            lcb = lcb.reset_index()
            df = pd.concat([df, lcb])

        # free bins
        q = f"SELECT pps_id,free_bins FROM pps_data WHERE time>='{self.start_date}' and time < '{self.end_date}' and mode = 'pick' and front_logged_in= 'true' "
        free_bins = pd.DataFrame(self.read_client.query(q).get_points())
        if not free_bins.empty:
            free_bins = free_bins.groupby(['pps_id']).agg(actual_value=('free_bins', 'last'), time=('time', 'last'))
            free_bins['actual_value'] = free_bins['actual_value'].astype(int)
            free_bins['expected_value'] = int(expected_value_df['r2r__free_bins'][0])
            free_bins['result'] = free_bins.apply(breached_function_reverse,axis=1)
            free_bins['parameter'] = parameter_func
            free_bins['insights'] = 'Free Bins available'
            free_bins['per_pps'] = 'YES'
            free_bins = free_bins.reset_index()
            df = pd.concat([df, free_bins])

        # waiting for bin clear
        q = f"select pps_id, occupied_bins, ongoing_bins from pps_data where time>='{self.start_date}' and time < '{self.end_date}' and mode='pick' and front_logged_in='true'  "
        bin_clear = pd.DataFrame(self.read_client.query(q).get_points())
        if not bin_clear.empty:
            bin_clear = bin_clear.groupby(['pps_id']).agg(ongoing=('ongoing_bins', 'last'),
                                                          occupied=('occupied_bins', 'last'), time=('time', 'last'))
            bin_clear['ongoing'] = bin_clear['ongoing'].astype(int)
            bin_clear['occupied'] = bin_clear['occupied'].astype(int)
            bin_clear['actual_value'] = bin_clear['occupied'] - bin_clear['ongoing']
            bin_clear['expected_value'] = int(expected_value_df['r2r__bins_waiting_for_clearing'][0])
            bin_clear['result'] = bin_clear.apply(breached_function, axis=1)
            bin_clear.drop(['ongoing', 'occupied'], axis=1, inplace=True)
            bin_clear['parameter'] = parameter_func
            bin_clear['insights'] = 'Bins Waiting for clearing'
            bin_clear['per_pps'] = 'YES'
            bin_clear = bin_clear.reset_index()
            df = pd.concat([df, bin_clear])

        # bots going to pps
        q = f'select value, pps_id from "pps_tasks/gotopps"' + f" where time>='{self.start_date}' and time < '{self.end_date}'"
        A = pd.DataFrame(self.read_client.query(q).get_points())
        if not A.empty:
            A = A.groupby(['pps_id']).agg(value=('value', 'mean'), time=('time', 'last'))
            A = A.reset_index()
            A['pps_id'] = A['pps_id'].astype(int)
        q = f"select physical_queue, pps_id from pps_queue_data where time>='{self.start_date}' and time < '{self.end_date}'  "
        B = pd.DataFrame(self.read_client.query(q).get_points())
        if not B.empty:
            B = B.groupby(['pps_id']).agg(physical_queue=('physical_queue', 'last'))

        if not A.empty and not B.empty:
            bots_to_pps = pd.merge(A, B, on=['pps_id'], how='inner')
            if not bots_to_pps.empty:
                bots_to_pps['actual_value'] = bots_to_pps['value'] / bots_to_pps['physical_queue']
                bots_to_pps['expected_value'] = int(expected_value_df['r2r__bot_going_to_pps'][0])
                bots_to_pps['result'] = bots_to_pps.apply(breached_function_reverse, axis=1)
                bots_to_pps['parameter'] = parameter_func
                bots_to_pps['insights'] = 'Bots going to PPS / Queue'
                bots_to_pps['per_pps'] = 'YES'
                bots_to_pps = bots_to_pps.reset_index()
                bots_to_pps.drop(['value', 'physical_queue'], axis=1, inplace=True)
                df = pd.concat([df, bots_to_pps])

        # high rack hoping
        q = f"select count(new_task_id) as rack_hop from same_rack_tasks where time>='{self.start_date}' and time < '{self.end_date}' "
        rack_hoping_A = pd.DataFrame(self.read_client.query(q).get_points())
        q = f"select count(event) as all_task from ppstask_events where time>='{self.start_date}' and time < '{self.end_date}' "
        rack_hoping_B = pd.DataFrame(self.read_client.query(q).get_points())

        if not rack_hoping_A.empty and not rack_hoping_B.empty:
            rack_hoping = {}
            A = rack_hoping_A['rack_hop'][0]
            B = rack_hoping_B['all_task'][0]
            rack_hoping['actual_value'] = [str(round(100*A/B,2))+'%']
            rack_hoping['expected_value'] = [str(expected_value_df['r2r__rack_hoping'][0])]
            rack_hoping['result'] = ['Non-Breached']
            rack_hoping['parameter'] = [parameter_func]
            rack_hoping['insights'] = ['High rack hopping']
            rack_hoping['per_pps'] =['NO']
            # rack_hoping = rack_hoping.reset_index()
            rack_hoping_1 = pd.DataFrame(rack_hoping)
            rack_hoping_1['result'] = rack_hoping_1.apply(breached_function, axis=1)
            df = pd.concat([df, rack_hoping_1])

        # bot in error
        q = f"select pps_id, bots_going_to_pps-effective_bots_going_to_pps as bot_in_error from pps_queue_data where time>='{self.start_date}' and time < '{self.end_date}'  "
        C = pd.DataFrame(self.read_client.query(q).get_points())
        if not C.empty:
            bot_error = C.groupby(['pps_id']).agg(bot_in_error=('bot_in_error', 'last'))
        if not C.empty:
            bot_error['actual_value'] = bot_error['bot_in_error']
            bot_error['expected_value'] = int(expected_value_df['r2r__bot_error'][0])
            bot_error['result'] = bot_error.apply(lambda x: 'Non-Breached' if x['actual_value'] == x['expected_value'] else 'Breached', axis=1)
            bot_error['parameter'] = parameter_func
            bot_error['insights'] = 'Bots in error while going to PPS'
            bot_error['per_pps'] = 'YES'
            # bot_error.drop(['effective_bots_going_to_pps', 'value'], axis=1, inplace=True)
            bot_error = bot_error.reset_index()
            df = pd.concat([df, bot_error])

        # order pool available
        q = f"SELECT last(total_bins) FROM pps_data WHERE time>='{self.start_date}' and time < '{self.end_date}' and mode = 'pick'  group by pps_id"
        order_pool = pd.DataFrame(self.read_client.query(q).get_points())
        if not order_pool.empty:
            order_pool['last'] = order_pool['last'].astype(int)
            expected_value = (order_pool['last'].sum()) * 2
            q = f"select sum(last) as actual_value from (SELECT last(orders) FROM outstanding_orders WHERE (status =~ /created/) AND time>='{self.start_date}' and time < '{self.end_date}'  group by bin_tags)"
            order_pool = pd.DataFrame(self.read_client.query(q).get_points())
            order_pool['expected_value'] = int(expected_value)
            order_pool['result'] = order_pool.apply(breached_function_reverse, axis=1)
            order_pool['parameter'] = parameter_func
            order_pool['insights'] = 'Order Pool Available'
            order_pool['per_pps'] = 'NO'
            order_pool = order_pool.reset_index()
            df = pd.concat([df, order_pool])
        # df = pd.concat([free_bots,conjection_factor,Bot_stop_highway,Bot_stop_pps,Bot_stop_aisle,Bot_stop_storable,lcb,free_bins,bin_clear,rack_hoping,bot_error,bots_to_pps,mle_alert,order_pool])
        df['actual_value'] = df['actual_value'].astype(str,errors='ignore')
        df['expected_value'] = df['expected_value'].astype(str,errors='ignore')
        df['actual_value'] = df['actual_value'].apply(lambda x: "-" if x=="nan%" or x=="nan" or pd.isna(x) else x)
        # df['actual_value'] = df['actual_value'].round(2)
        if 'index' in df:
            del df['index']
        # df.time = self.end_date
        self.end_date = pd.to_datetime(self.end_date)
        # df = df.set_index(pd.to_datetime(np.arange(len(df))*-1, unit='ns', origin=self.end_date))
        # df = df.set_index('time')
        df.time = pd.to_datetime(np.arange(len(df)) * -1, unit='s', origin=self.end_date)
        df['pps_id']=df.apply(lambda x:'0' if x['per_pps']=='NO' else x['pps_id'], axis=1)
        df['pps_id'] = df['pps_id'].astype(str,errors='ignore')
        df = df.set_index('time')
        self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                             port=self.tenant_info["write_influx_port"])
        self.write_client.writepoints(df, "r2r_insight", db_name=self.tenant_info["alteryx_out_db_name"],
                                      tag_columns=['insights'], dag_name=os.path.basename(__file__),
                                      site_name=self.tenant_info['Name'])


with DAG(
        'R2R_insights',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='Gives insights about high R2R time',
        schedule_interval='*/15 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(minutes=60)
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['r2r_insights'] == "Y"  and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                final_task = PythonOperator(
                    task_id='r2r_insights_final_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(R2R_insights().final_call, tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":Butler_ip, "influx_ip":influx_ip, "influx_port":influx_port,\
        #           "write_influx_ip":write_influx_ip,"write_influx_port":influx_port, \
        #           "out_db_name":db_name}
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='r2r_insights_final',
            provide_context=True,
            python_callable=functools.partial(R2R_insights().final_call, tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )
