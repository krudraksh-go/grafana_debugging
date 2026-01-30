## -----------------------------------------------------------------------------
## Import deps
## -----------------------------------------------------------------------------

import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import InfluxData, CommonFunction, postgres_connection
## -----------------------------------------------------------------------------
## DAG defination
## -----------------------------------------------------------------------------

import os

Butler_ip = os.environ.get('MNESIA_IP', 'localhost')
influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
influx_port = os.environ.get('INFLUX_PORT', '8086')
write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.11.4.23')
db_name = os.environ.get('Out_db_name', 'airflow')


## -----------------------------------------------------------------------------
## python callable definations
## -----------------------------------------------------------------------------

class task_cycle_times_summary:

    def create_time_series_data(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df        
    
    def find_last_entry(self):
        influx = self.CommonFunction.get_central_influx_client(self.tenant_info['alteryx_out_db_name'])
        df = influx.fetch_one("""select * from task_cycle_times_summary where time>now()-40d and  
                    site='{site}'  order by time desc limit 1""".format(
            site=self.tenant_info['Name'])).reset_index().rename(columns={"index": "time"})
        if not df.empty:
            last_entry = df['time'].iloc[0]
        else:
            last_entry = datetime.now(timezone.utc)- timedelta(days=40)
        return last_entry


    def get_custom_daterange(self):
        start_date = self.find_last_entry()
        end_date = datetime.now(timezone.utc)
        timeseriesdata = self.create_time_series_data(start_date,end_date, '1h')
        return timeseriesdata


    def find_type(self, butler_version):
        if 'TTP' in butler_version.upper():
            return 'TTP'
        elif 'RELAY' in butler_version.upper():
            return 'RELAY'
        else:
            return 'RTP'

    def fetch_data_from_postgres(self, query, columns):
        if self.tenant_info['Postgres_butler_user'] != "":
            self.postgres_conn = postgres_connection(database='butler_dev',
                                                            user=self.tenant_info['Postgres_butler_user'], \
                                                            sslrootcert=self.tenant_info['sslrootcert'],
                                                            sslcert=self.tenant_info['sslcert'], \
                                                            sslkey=self.tenant_info['sslkey'],
                                                            host=self.tenant_info['Postgres_ButlerDev'], \
                                                            port=self.tenant_info['Postgres_butler_port'],
                                                            password=self.tenant_info['Postgres_butler_password'],
                                                            dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
            data = self.postgres_conn.fetch_postgres_data_in_chunk(query)
            self.postgres_conn.close()
            df = pd.DataFrame(data, columns=columns)
            return df
        return pd.DataFrame( columns=columns)


    def get_reservation_offset_metrics(self, start_date, end_date):
        reservation_offset_start_time_query = f""" SELECT 
                                                        butler_version,
                                                        AVG(
                                                            CASE 
                                                                WHEN path_calc_reservation_start_time IS NOT NULL
                                                                     AND path_calc_reservation_start_time IS NOT NULL
                                                                     AND path_calc_reservation_start_time != -1
                                                                     AND exclusive_reservation_start_time IS NOT NULL
                                                                     AND exclusive_reservation_start_time != -1
                                                                THEN ABS(exclusive_reservation_start_time - path_calc_reservation_start_time)
                                                                ELSE NULL
                                                            END
                                                        ) AS start_time_diff,
                                                        
                                                        AVG(
                                                            CASE 
                                                                WHEN path_calc_reservation_end_time IS NOT NULL
                                                                     AND path_calc_reservation_end_time != -1
                                                                     AND exclusive_reservation_end_time IS NOT NULL
                                                                     AND exclusive_reservation_end_time != -1
                                                                THEN ABS(path_calc_reservation_end_time - exclusive_reservation_end_time)
                                                                ELSE NULL
                                                            END
                                                        ) AS end_time_diff,
                                                        
                                                        AVG(
                                                            CASE 
                                                                WHEN path_calc_arrival_time IS NOT NULL
                                                                     AND path_calc_arrival_time != -1
                                                                     AND path_exec_arrival_time IS NOT NULL
                                                                     AND path_exec_arrival_time != -1
                                                                THEN ABS(path_calc_arrival_time - path_exec_arrival_time)
                                                                ELSE NULL
                                                            END
                                                        ) AS arrival_time_diff
                                                    FROM 
                                                        path_res_offset_data
                                                    WHERE 
                                                        created_at BETWEEN '{start_date}' AND '{end_date}'
                                                    GROUP BY 
                                                        butler_version;
                                                     """

        # reservation_offset_end_time_query = f""" SELECT butler_version,
        #                                         avg(abs(path_calc_reservation_end_time - exclusive_reservation_end_time)) as end_time_diff
        #                                         FROM
        #                                         path_res_offset_data
        #                                         WHERE
        #                                         path_calc_reservation_end_time is not null AND
        #                                         path_calc_reservation_end_time != -1 AND
        #                                         exclusive_reservation_end_time is not null AND
        #                                         exclusive_reservation_end_time != -1
        #                                         AND created_at BETWEEN '{start_date}' AND '{end_date}' group by butler_version """
        #
        #
        # reservation_offset_arrival_time_query = f""" SELECT butler_version,
        #                                         avg(abs(path_calc_arrival_time - path_exec_arrival_time)) as arrival_time
        #                                         FROM
        #                                         path_res_offset_data
        #                                         WHERE
        #                                         path_calc_arrival_time is not null AND
        #                                         path_calc_arrival_time != -1 AND
        #                                         path_exec_arrival_time is not null AND
        #                                         path_exec_arrival_time != -1
        #                                         AND created_at BETWEEN '{start_date}' AND '{end_date}' group by butler_version """

        try:
            res = self.fetch_data_from_postgres(reservation_offset_start_time_query, columns=['butler_version', 'start_time_diff','end_time_diff','arrival_time'])
        except:
            res =pd.DataFrame(columns=['butler_version', 'start_time_diff','end_time_diff','arrival_time'])
            res['butler_version']='RTP'
            res['start_time_diff'] = 0
            res['end_time_diff'] = 0
            res['arrival_time'] = 0
        # reservation_offset_end_time_diff = self.fetch_data_from_postgres(reservation_offset_end_time_query,'end_time_diff')
        # reservation_offset_arrival_time_diff = self.fetch_data_from_postgres(reservation_offset_arrival_time_query,'arrival_time')

        # res = pd.merge(reservation_offset_start_time_diff, reservation_offset_end_time_diff, on = ['butler_version'], how = 'outer' )
        # res = pd.merge(res, reservation_offset_arrival_time_diff, on = ['butler_version'], how = 'outer' )
        res['type'] = res['butler_version'].apply(lambda x : self.find_type(x))
        del res['butler_version']
        return res


    def task_cycle_times_summary_final(self, tenant_info, **kwargs):
        self.tenant_info = tenant_info['tenant_info']
        self.site = self.tenant_info['Name']
        self.CommonFunction = CommonFunction()
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db=self.tenant_info["out_db_name"])

        isvalid = self.read_client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        check_start_date = self.read_client.get_start_date("task_cycle_times_summary", self.tenant_info)
        check_end_date = datetime.now(timezone.utc)
        check_start_date = pd.to_datetime(check_start_date) + timedelta(minutes=1)  # corner case
        check_start_date = pd.to_datetime(check_start_date).strftime("%Y-%m-%d %H:%M:%S")
        check_end_date = pd.to_datetime(check_end_date).strftime("%Y-%m-%d %H:%M:%S")

        q = f"select *  from task_cycle_times where time>'{check_start_date}' and time<='{check_end_date}' and event_name = 'goto_completed' limit 1"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            self.end_date = datetime.now(timezone.utc)
            self.task_cycle_times_summary_final1(self.end_date, **kwargs)
        else:
            try:
                daterange = self.get_custom_daterange()
                for i in daterange.index:
                    self.end_date = daterange['end_date'][i]
                    self.task_cycle_times_summary_final1(self.end_date, **kwargs)
            except AirflowTaskTimeout as timeout_exception:
                raise timeout_exception
            except Exception as e:
                print(f"error:{e}")
                raise e

    def task_cycle_times_summary_final1(self, end_date, **kwargs):
        self.start_date = self.find_last_entry()
        self.start_date = self.start_date.replace(second=0)
        self.start_date = self.start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_date = end_date
        self.end_date = self.end_date.replace(second=0)
        self.end_date = self.end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(self.start_date, self.end_date)

        q = f"select * from task_cycle_times where time>'{self.start_date}' and time <= '{self.end_date}' and event_name = 'goto_completed' and path_type!='pps_entry_to_pps' and host = '{self.tenant_info['host_name']}' order by time desc"
        df = pd.DataFrame(self.read_client.query(q).get_points())
        if df.empty:
            return None

        site = self.tenant_info['Name']
        flow = self.tenant_info['flow']
        df['type'] = df['butler_version'].apply(lambda x: self.find_type(x))
        butler_version = df.groupby(['butler_id'], as_index=False).agg(
            type = ('type','first')
        )        

        agg_df = df.groupby(['type'], as_index=False).agg(
            time = ('time','max'),
            min_deadlock_resolution_time = ('deadlock_resolution_time','min'),
            sum_deadlock_resolution_time = ('deadlock_resolution_time','sum'),
            avg_deadlock_resolution_time = ('deadlock_resolution_time','mean'),
            min_deadlock_counts = ('deadlock_counts','min'),
            sum_deadlock_counts = ('deadlock_counts','sum'),
            avg_deadlock_counts = ('deadlock_counts','mean'),
            sum_path_travel_time = ('total_path_travel_time','sum'),
            sum_original_idc_time = ('original_idc_time','sum'),
            number_of_tasks = ('event_name','count'),
            number_of_bots = ('butler_id','nunique'),
            number_of_steps = ('step_count','sum')
        ).reset_index(drop=True)

        agg_df['deadlock_percentage'] = round((agg_df['sum_deadlock_resolution_time'] / agg_df['sum_path_travel_time']) * 100, 2)
        agg_df['nav_efficiency'] = (agg_df['sum_path_travel_time'] / agg_df['sum_original_idc_time']) 

        breach_df = df[df['original_path_type']!='pps_entry_to_pps'].groupby(['type','task_key'], as_index=False).agg(
            sum_total_path_travel_time = ('total_path_travel_time','sum'),
            sum_original_idc_time = ('original_idc_time','sum')
        ).reset_index(drop=True)
        breach_df['factor'] = breach_df['type'].apply(lambda x : 2 if x=='TTP' else 1.5 )
        breach_df['path_time_diff'] = breach_df.apply(lambda x : x['sum_total_path_travel_time'] - x['sum_original_idc_time'] * x['factor'], axis =1)
        breach_df['breach_flag'] = breach_df['path_time_diff'].apply(lambda x : 1 if x > 30 else 0)

        breach_df = breach_df.groupby(['type'], as_index=False).agg(
            BreachesCount = ('breach_flag','sum'),
            TotalCycle = ('breach_flag','count')
        ).reset_index(drop=True)
        breach_df['breach_percentage'] = round((breach_df['BreachesCount'] / breach_df['TotalCycle']) * 100, 2)
        summary_info = pd.merge(agg_df, breach_df, on='type', how='left')


        # deadlock stats Metrics
        last_entry_time = summary_info['time'].max()
        q = f"select * from deadlock_stats where time > '{self.start_date}' and time <= '{last_entry_time}'  and host = '{self.tenant_info['host_name']}' "
        deadlock_stats_df = pd.DataFrame(self.read_client.query(q).get_points())
        if not deadlock_stats_df.empty:
            deadlock_stats_df['butler_id'] = deadlock_stats_df['butler_id'].astype(str)
            butler_version['butler_id'] = butler_version['butler_id'].astype(str)      
            deadlock_stats_df = pd.merge(deadlock_stats_df,butler_version, on = 'butler_id', how = 'left')
            deadlock_stats_df['created'] = deadlock_stats_df['event'].apply(lambda x : 1 if x=='created' else 0)
            deadlock_stats_df['idle_resolved'] = deadlock_stats_df['event'].apply(lambda x : 1 if x=='idle_resolved' else 0)
            deadlock_stats_df['idle_failed'] = deadlock_stats_df['event'].apply(lambda x : 1 if x=='idle_failed' else 0)
            deadlock_stats_df['false_detection'] = deadlock_stats_df['event'].apply(lambda x : 1 if x=='false_detection' else 0)
            deadlock_stats_df = deadlock_stats_df.groupby(['type'], as_index=False).agg(
                Deadlock_creation_count = ('created','sum'),
                Idle_resolved_count = ('idle_resolved','sum'),
                Idle_failed_count = ('idle_failed','sum'),
                False_detection_count = ('false_detection','sum'),
            )
        else:
            deadlock_stats_df = pd.DataFrame(columns = ['type','Deadlock_creation_count','Idle_resolved_count','Idle_failed_count','False_detection_count'])
        
        summary_info = pd.merge(summary_info, deadlock_stats_df, on = 'type', how = 'outer')
        


        # Reservation Offset Metrics
        reservation_offset = self.get_reservation_offset_metrics(self.start_date, last_entry_time)
        summary_info = pd.merge(reservation_offset, summary_info, on = ['type'], how = 'outer')
        summary_info = summary_info.fillna(0)
        summary_info.time = pd.to_datetime(summary_info.time)
        summary_info = summary_info.set_index('time')


        # create path_type_summary
        path_type_summary = df.groupby(['type','path_type'], as_index=False).agg(
                            time = ('time','max'),
                            sum_path_travel_time = ('total_path_travel_time','sum'),
                            sum_original_idc_time = ('original_idc_time','sum'),
                            number_of_tasks = ('event_name','count'),
                            number_of_bots = ('butler_id','nunique'),
                        ).reset_index(drop=True)

        path_type_summary['nav_efficiency'] = path_type_summary.apply(lambda x : round(x['sum_path_travel_time'] / x['sum_original_idc_time'],2) if x['sum_original_idc_time'] >0 else 0, axis = 1)
        path_type_summary['factor'] = path_type_summary['type'].apply(lambda x : 2 if x=='TTP' else 1.5 )
        path_type_summary['path_time_diff'] = path_type_summary.apply(lambda x: x['sum_path_travel_time'] - x['factor'] * x['sum_original_idc_time'], axis = 1)
        path_type_summary['breach'] = path_type_summary.apply(lambda x : 1 if (x['path_type']=='ttp_storable_io_point_to_ttp_storable_io_point' and x['path_time_diff']>5) or (x['path_time_diff']>30) else 0, axis = 1)
        path_type_summary['breach_percentage'] = round( 100 *(path_type_summary['breach'] / path_type_summary['number_of_tasks']) ,2)

        path_type_summary.time = pd.to_datetime(path_type_summary.time)
        path_type_summary = path_type_summary.set_index('time')     

        summary_info['site'] = site
        summary_info['solution_type'] = flow

        path_type_summary['site'] = site
        path_type_summary['solution_type'] = flow   

        str_cols = ['site','solution_type','type']
        float_cols = []
        for col in summary_info.columns:
            if col not in str_cols:
                float_cols.append(col)
        summary_info = self.CommonFunction.str_typecast(summary_info,str_cols)
        summary_info = self.CommonFunction.float_typecast(summary_info,float_cols)   

        summary_info['key'] = summary_info['site'] + '-' + summary_info['type']

        str_cols = ['site','solution_type','type','path_type']
        float_cols = []
        for col in path_type_summary.columns:
            if col not in str_cols:
                float_cols.append(col)
        path_type_summary = self.CommonFunction.str_typecast(path_type_summary,str_cols)
        path_type_summary = self.CommonFunction.float_typecast(path_type_summary,float_cols)   
        path_type_summary['key'] = path_type_summary['site']  + '-' + path_type_summary['type']

        write_influx = self.CommonFunction.get_central_influx_client(self.tenant_info['alteryx_out_db_name'])
        isvalid= write_influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])
        if isvalid:
            success = write_influx.write_all(summary_info, 'task_cycle_times_summary', dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], tag_columns=["site","type"])
            success = write_influx.write_all(path_type_summary, 'task_cycle_times_path_type_summary', dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'], tag_columns=["site","path_type","type"])
        print("inserted")
        return

    





with DAG(
        'task_cycle_times_summary',
        default_args=CommonFunction().get_default_args_for_dag(),
        description='task_cycle_times_summary dag created',
        schedule_interval='15 * * * *',
        max_active_runs=1,
        max_active_tasks=16,
        concurrency=16,
        catchup=False,
        dagrun_timeout=timedelta(seconds=3600),
) as dag:
    import csv
    import os
    import functools

    if os.environ.get('MULTI_TENANT_DAGS', 'false') == 'true':
        csvReader = CommonFunction().get_all_site_data_config()
        for tenant in csvReader:
            if tenant['Active'] == "Y" and tenant['task_cycle_times_summary'] == "Y" and (tenant['is_production'] == "Y" or tenant['IsIndivisualInflux'] == "Y"):
                try:
                    final_task = PythonOperator(
                        task_id='task_cycle_times_summary_final_{}'.format(tenant['Name']),
                        provide_context=True,
                        python_callable=functools.partial(task_cycle_times_summary().task_cycle_times_summary_final,
                                                          tenant_info={'tenant_info': tenant}),
                        execution_timeout=timedelta(seconds=3600),
                    )
                except AirflowTaskTimeout as timeout_exception:
                    raise timeout_exception
                except Exception as e:
                    print(f"error:{e}")
                    raise e

    else:
        tenant = CommonFunction().get_tenant_info()
        final_task = PythonOperator(
            task_id='task_cycle_times_summary_final',
            provide_context=True,
            python_callable=functools.partial(task_cycle_times_summary().task_cycle_times_summary_final,
                                              tenant_info={'tenant_info': tenant}),
            execution_timeout=timedelta(seconds=3600),
        )

