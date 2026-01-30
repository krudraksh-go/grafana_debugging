import pandas as pd
import json
from functools import partial

import os
import airflow
from airflow import DAG

from utils.CommonFunction import SingleTenantDAGBase, MultiTenantDAG, average, postgres_connection, CommonFunction, Write_InfluxData
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

from config import INFLUX_DATETIME_FORMAT
import time

dag = DAG(
    "daily_site_kpi",
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval='1 7 */1 * *',
    default_args=CommonFunction().get_default_args_for_dag(),
    catchup=False,
)


class DailySiteKPI():

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df

    def get_datetime_interval2(self):
        influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'),
                                                               self.site.get('central_influx'))
        start_datetime = influx.fetch_one("""select * from daily_site_kpi_ayx 
                      where site='{site}'  order by time desc limit 1""".format(
            site=self.site.get('name')))
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=1)
        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = datetime.now(timezone.utc)
        timeseriesdata = self.create_time_series_data2(start_datetime, end_datetime, '1d')
        return timeseriesdata

    def get_datetime_interval3(self, end_date):
        influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'),
                                                               self.site.get('central_influx'))
        start_datetime = influx.fetch_one("""select * from daily_site_kpi_ayx 
                      where site='{site}'  order by time desc limit 1""".format(
            site=self.site.get('name')))
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
            start_datetime = start_datetime + timedelta(days=1)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=3)
        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = end_date
        timeseriesdata = self.create_time_series_data2(start_datetime, end_datetime, '1d')
        return timeseriesdata

    def get_datetime_interval(self, sheet_startdate, interval, midnight=None):
        sheet_datetime = datetime.strptime(sheet_startdate, INFLUX_DATETIME_FORMAT)
        # now = datetime.now()
        # influx = self.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        # start_datetime = influx.fetch_one("""select * from daily_site_kpi_ayx
        #     where site='{site}' and time<'2023-03-23T07:00:00Z' order by time desc limit 1""".format(site=self.site.get('name')))
        # start_datetime= pd.to_datetime(start_datetime.index[0],utc=True)
        #
        # end_datetime = datetime.strptime("{}-{}-{} {}:{}:00".format(
        #     now.year, now.month, now.day, sheet_datetime.hour, sheet_datetime.minute
        # ), "%Y-%m-%d %H:%M:%S")
        # start_datetime = end_datetime - timedelta(days=int(interval/(24 * 60 * 60)))
        start_datetime = pd.to_datetime(self.start_date, utc=True)
        end_datetime = pd.to_datetime(self.end_date, utc=True)
        hour, minute, second = map(str, self.site.get('starttime').split(':'))
        hour = hour + "h" + minute + "m"

        if midnight:
            start_datetime = start_datetime.replace(hour=0, minute=0)
            end_datetime = end_datetime.replace(hour=0, minute=0)

        return (
            datetime.strftime(start_datetime, INFLUX_DATETIME_FORMAT),
            datetime.strftime(end_datetime, INFLUX_DATETIME_FORMAT),
            hour
        )

    def get_extra_where(self):
        if self.site.get('name') == 'SBS':
            return 'and pps_id=~/8|9|10|11|12|13|14|15|16|17|18|19|20/'
        return ''

    def get_params(self, midnight=None):
        start_datetime, end_datetime, hour = self.get_datetime_interval(
            '2020-04-01 07:00:00',
            86400,
            midnight
        )
        return {
            'start_datetime': start_datetime,
            'end_datetime': end_datetime,
            'hour': hour,
            'extra_where': self.get_extra_where(),
        }

    def fetch_data(self, query, fetch_one=True, midnight=None, format="json", get_all_data=False):
        influx = self.CommonFunction.get_site_read_influx_client(site=self.site, database='GreyOrange')
        formatted_query = query.format(**self.get_params(midnight))
        print(formatted_query)
        # print("formatted_query", formatted_query)
        if fetch_one:
            return influx.fetch_one(formatted_query, format=format)
        return influx.fetch_all(formatted_query, format=format, get_all_data=get_all_data)

    def get_agg_pps_operator_log(self):

        # data = self.fetch_data("""
        #         select time_diff_b_w_pptl_press_to_rack_arrival/1000 as value from Alteryx..operator_working_time_summary_airflow
        #         where time_diff_b_w_pptl_press_to_rack_arrival<550000 and
        #             time >= '{start_datetime}' and time < '{end_datetime}'
        #     """,
        #     fetch_one=False,
        #     format="dataframe"
        # )

        # if data.empty:
        data = self.fetch_data("""
                select value/1000 as value from Alteryx..r2r_waittime_calculation
                where  event='rack_arrived_to_pps'  and value<550000 and task_type='pick' and 
                    time >= '{start_datetime}' and time < '{end_datetime}'
            """,
                               fetch_one=False,
                               format="dataframe"
                               )

        if data.empty:
            data = self.fetch_data("""
                    select * from pps_operator_log
                    where pps_mode = 'pick_mode' and type='waiting_time' 
                        and value>3 and value<550 and 
                        time >= '{start_datetime}' and time < '{end_datetime}'
                """,
                                   fetch_one=False,
                                   format="dataframe"
                                   )
        if data.empty:
            return {
                'Avg_value': 0,
                'Avg_90': 0,
                'Avg_95': 0,
                'Avg_99': 0,
                'RTR_Sum': 0,
                'RTR_Count': 0
            }

        data['RecordID'] = range(1, len(data) + 1)
        data = data.sort_values(by=['value'], axis=0)
        record_count = len(data.index)

        data['90'] = data.apply(
            lambda row: row['value'] if row['RecordID'] <= record_count * 0.9 else 0, axis=1
        )
        data['95'] = data.apply(
            lambda row: row['value'] if row['RecordID'] <= record_count * 0.95 else 0, axis=1
        )
        data['99'] = data.apply(
            lambda row: row['value'] if row['RecordID'] <= record_count * 0.99 else 0, axis=1
        )

        return {
            'Avg_value': average(data['value'].tolist()),
            'Avg_90': average(data['90'].tolist()),
            'Avg_95': average(data['95'].tolist()),
            'Avg_99': average(data['99'].tolist()),
            'RTR_Sum': sum(data['value'].tolist()),
            'RTR_Count': len(data['value'].tolist())
        }

    def get_mean_pps_operator(self):
        data = self.fetch_data(
            """ select mean(value)/1000 as mean_pps_operator
                from Alteryx..r2r_waittime_calculation
                where task_type = 'pick' and event = 'rack_arrived_to_pps' 
                and time >= '{start_datetime}' and time < '{end_datetime}'
                and value < 550000 group by time(1d, {hour})""",
        )
        if data == {}:
            return {'mean_pps_operator': 0}
        return data

    def get_time_to_pick_one_time(self):
        data = self.fetch_data(
            """ select (sum(total_time)/sum(quantity)) as time_to_pick_one_time
                from flow_transactions_sec_alteryx
                where time >= '{start_datetime}' and time < '{end_datetime}'
                {extra_where} and total_time < 600 group by time(1d, {hour})""",
        )
        if data == {}:
            return {
                'time_to_pick_one_time': 1
            }
        return data

    def get_picks_per_face(self):
        data = self.fetch_data(
            """ select
                    mean(total_picks) as picks_per_face,
                    mean(total_picks_value) as picks_per_face_per_item,
                    sum(total_picks) as SUM_picks_per_face,
                    count(total_picks) as COUNT_picks_per_face
                from picks_per_rack_face
                where
                    time >= '{start_datetime}' and time < '{end_datetime}'
                    {extra_where} group by time(1d, {hour})""",
        )
        if data == {} or data == []:
            return {
                'picks_per_face': 0,
                'SUM_picks_per_face': 0,
                'COUNT_picks_per_face': 0,
                'picks_per_face_per_item': 0
            }

        return data

    def get_pick_UPH_60min(self):
        data = self.fetch_data(""" select
                    mean(*),
                    sum(UPH_60min) as SUM_UPH_60min,
                    count(UPH_60min) as COUNT_UPH_60min
                from interval_throughput
                where time >= '{start_datetime}' and time < '{end_datetime}' and 
                UPH_60min > 0 {extra_where} group by time(1d, {hour})""",
                               )
        if data == {}:
            return {
                'SUM_UPH_60min': 0,
                'COUNT_UPH_60min': 0,
                'mean_UPH_15min': 0,
                'mean_UPH_30min': 0,
                'mean_UPH_60min': 0
            }
        return data

    def get_count_butler(self):
        data = self.fetch_data("""
                select count(distinct(bot_id_int)) as Count_butler
                from butler_uptime
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'Count_butler': 0}
        return data

    def get_put_UPH_60min(self):
        data = self.fetch_data("""select
                mean(*),
                sum(UPH_60min) as SUM_put_UPH_60min,
                count(UPH_60min) as COUNT_put_UPH_60min
            from put_interval_throughput
            where time >= '{start_datetime}' and time < '{end_datetime}' and
                UPH_60min > 0 group by time(1d, {hour})""",
                               )
        data['put_UPH_60min'] = data.get('mean_UPH_60min')
        if data == {}:
            return {
                'SUM_put_UPH_60min': 0,
                'COUNT_put_UPH_60min': 0,
                'put_UPH_60min': 0
            }
        return data

    def get_put_per_face(self):
        data = self.fetch_data("""
                select mean(total_put) as put_per_face,
                    sum(total_put) as SUM_put_per_face,
                    COUNT(total_put) as COUNT_put_per_face,
                    mean(total_put_value) as put_per_face_per_item
                from put_per_rack_face
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'put_per_face': 0, 'SUM_put_per_face': 0, 'COUNT_put_per_face': 0, 'put_per_face_per_item': 0}
        return data

    def get_put_items(self):
        data = self.fetch_data("""
                select * from item_put
               where time >= '{start_datetime}' and time < '{end_datetime}'""",
                               fetch_one=False,
                               format='dataframe'
                               )

        if data.empty:
            return {'CBM': 0}

        return {
            'CBM': average(data['volume_int'].tolist()) if 'volume_int' in data.columns else 0.0
        }

    def get_put_per_face_agg(self):
        data = self.fetch_data("""
                select
                    sum(value) as total_item_put,
                    count(put_id)/count(distinct(put_id)) as tx_expectation,
                    sum(uom_quantity_int)/count(item_id) as avg_item_per_tx,
                    sum(volume_int) as CBM_put_actual,
                    count(distinct(slotref)) as slots_used
                from item_put
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'total_item_put': 0, 'tx_expectation': 0, 'avg_item_per_tx': 0, 'CBM_put_actual': 0,
                    'slots_used': 0}
        return data

    def get_warehouse_occupancy(self):
        data = self.fetch_data("""
                select
                    last(value) as avg_utilization,
                    last(value)*100 as avg_utilization_percent,
                    last(warehouse_space_total) as total_warehouse_cap_CBM
                from warehouse_occupancy
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'total_warehouse_cap_CBM': 0, 'avg_utilization': 0, 'avg_utilization_percent': 0}

        data['total_warehouse_cap_CBM'] = float(data['total_warehouse_cap_CBM']) if not pd.isna(
            data['total_warehouse_cap_CBM']) else 0
        return data

    def get_total_put_exception_events(self):
        data = self.fetch_data("""
            select count(pps) as total_exceptions from put_exception_events
            where time >= '{start_datetime}' and time < '{end_datetime}'
            group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'total_exceptions': 0}
        return data

    def get_pps_operator_agg(self):
        data = self.fetch_data("""
                select
                    mean(value)/1000 as put_rack_to_rack,
                    sum(value)/1000 as SUM_put_rack_to_rack,
                    count(value) as COUNT_put_rack_to_rack
                from Alteryx..r2r_waittime_calculation
                where  task_type = 'put'  and event='rack_arrived_to_pps' and value<550000 and
                time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'put_rack_to_rack': 0, 'SUM_put_rack_to_rack': 0, 'COUNT_put_rack_to_rack': 0}
        return data

    def get_total_rack_pr(self):
        data = self.fetch_data("""
                select count(rack) as total_rack_pr
                from put_per_rack_face
                where 
                    time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'total_rack_pr': 0}
        return data

    def get_total_put_expectations(self):
        data = self.fetch_data("""
                select count(distinct(put_id)) as total_put_expectations
                from put_events
                where event_name='put_created' and
                    time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'total_put_expectations': 0}
        return data

    def get_time_to_put_one_item(self):
        data = self.fetch_data("""
                select mean(*) as time_to_put_one_item
                from pps_operator_log
                where type='waiting_time' and pps_mode='put_mode' and
                    time >= '{start_datetime}' and time < '{end_datetime}' and value < 600
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'time_to_put_one_item': 0}
        return data

    def get_order_tasked(self):
        data = self.fetch_data("""
                select count(order_id) as order_tasked
                from order_events
                where time >= '{start_datetime}' and time < '{end_datetime}' and
                    event_name='order_tasked'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'order_tasked': 0}
        return data

    def get_order_created(self):
        data = self.fetch_data("""
                select count(order_id) as order_created
                from order_events
                where time >= '{start_datetime}' and time < '{end_datetime}' and
                    event_name='order_created'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {'order_created': 0}
        return data

    def check_influx_server(self):
        influx = self.CommonFunction.get_site_read_influx_client(site=self.site, database='GreyOrange')
        valid = influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
        return valid

    def get_order_completed(self):
        data = self.fetch_data("""
                select count(order_id) as order_completed
                from order_events
                where time >= '{start_datetime}' and time < '{end_datetime}' and
                    event_name='order_complete'
                group by time(1d, {hour})""",
                               )
        if data == {}:
            return {
                'order_completed': 0
            }
        return data

    def get_site_working_hours(self):
        data = self.fetch_data("""
                select last(working_hr) as site_working_hr
                from Alteryx..site_working_time
                where time >= '{start_datetime}' and time < '{end_datetime}' """,
                               )
        if data == {}:
            return {
                'site_working_hr': 0
            }
        return data

    def get_port_entry_totes_presented(self):
        data = self.fetch_data("""
                    SELECT sum(tote_count) as port_entry_totes_presented
                     FROM "port_load_unload_events"
                    WHERE event = 'port_unloading_at_conveyor_started' and port_type='entry' 
                    and time >= '{start_datetime}' and time < '{end_datetime}' """,
                               )
        if data == {}:
            return {
                'port_entry_totes_presented': 0
            }
        return data

    def get_port_exit_totes_processed(self):
        data = self.fetch_data("""
                    SELECT count(event) as port_exit_totes_presented
                    FROM "port_load_unload_events"
                    WHERE event = 'conveyor_to_port_lift_transfer_completed' and port_type='exit'
                    and time >= '{start_datetime}' and time < '{end_datetime}' """,
                               )
        if data == {}:
            return {
                'port_exit_totes_presented': 0
            }
        return data

    def get_port_entry_errors(self):
        data = self.fetch_data("""
                        select count(health_status) as port_entry_errors
                        from port_events
                        where health_status = 'exception' and location_type = 'entry'
                        and time >= '{start_datetime}' and time < '{end_datetime}' """,
                               )
        if data == {}:
            return {
                'port_entry_errors': 0
            }
        return data

    def get_port_exit_errors(self):
        data = self.fetch_data("""
                        select count(health_status) as port_exit_errors
                        from port_events
                        where health_status = 'exception' and location_type = 'exit'
                        and time >= '{start_datetime}' and time < '{end_datetime}' """,
                               )
        if data == {}:
            return {
                'port_exit_errors': 0
            }
        return data

    def get_order_events(self):
        order_cnt = 1
        if self.order_event_data.empty:
            data = self.fetch_data("""
                               select count(order_id) as order_cnt
                               from order_events
                               where time >= '{start_datetime}' and time < '{end_datetime}'""",
                                   fetch_one=False, format="dataframe"
                                   )
            if not data.empty:
                order_cnt = data['order_cnt'][0]

            offset_val = 0
            data_limit = 5000
            while offset_val < order_cnt:
                qry = """select order_id, breach_state, event_name from order_events
                                       where time >= '{start_datetime}' and time < '{end_datetime}' order by time desc limit %s offset %s 
                                   """ % (data_limit, offset_val)
                temp_data = self.fetch_data(qry, fetch_one=False, format="dataframe")
                if offset_val == 0:
                    data = temp_data
                else:
                    data = pd.concat([data, temp_data])
                offset_val = offset_val + data_limit

            self.order_event_data = data
            data['time'] = data.index
            data = json.loads(data.to_json(orient='records'))
        else:
            data = json.loads(self.order_event_data.to_json(orient='records'))
        return data

    def get_order_created_at_risk(self, **kwargs):
        order_events = self.get_order_events()

        if len(list(filter(
                lambda x: x.get('event_name') == 'order_created' and x.get('breach_state') == 'risk_of_breach',
                order_events
        ))) == 0:
            query = """select count(order_id) as order_created_at_risk from order_events
                        where time >= '{start_datetime}' and time < '{end_datetime}'
                            and event_name='order_created'
                        group by time(1d, {hour})"""
        else:
            query = """select count(order_id) as order_created_at_risk from order_events
                        where time >= '{start_datetime}' and time < '{end_datetime}'
                            and event_name='order_created' and breach_state='risk_of_breach'
                        group by time(1d, {hour})"""
        data = self.fetch_data(query)
        return data

    def get_order_tasked_at_risk(self, **kwargs):
        order_events = self.get_order_events()

        if len(list(filter(
                lambda x: x.get('event_name') == 'order_tasked' and x.get('breach_state') == 'risk_of_breach',
                order_events
        ))) == 0:
            query = """select count(order_id) as order_tasked_at_risk from order_events
                        where time >= '{start_datetime}' and time < '{end_datetime}'
                            and event_name='order_created'
                        group by time(1d, {hour})"""
        else:
            query = """select count(order_id) as order_tasked_at_risk from order_events
                        where time >= '{start_datetime}' and time < '{end_datetime}'
                            and event_name='order_tasked' and breach_state='risk_of_breach'
                        group by time(1d, {hour})"""
        data = self.fetch_data(query)
        return data

    def get_order_tasked_breached(self, **kwargs):
        order_events = self.get_order_events()

        if len(list(filter(
                lambda x: x.get('event_name') == 'order_tasked' and x.get('breach_state') == 'breached',
                order_events
        ))) == 0:
            query = """select count(order_id) as order_tasked_breached from order_events
                        where time >= '{start_datetime}' and time < '{end_datetime}'
                            and event_name='order_created'
                        group by time(1d, {hour})"""
        else:
            query = """select count(order_id) as order_tasked_breached from order_events
                        where time >= '{start_datetime}' and time < '{end_datetime}'
                            and event_name='order_tasked' and breach_state='breached'
                        group by time(1d, {hour})"""
        data = self.fetch_data(query)
        return data

    def get_bot_maintenance_data(self):
        site = self.site.get('name')
        data = self.fetch_data("""
                select * from bot_maintenance_data
                where time >= '{start_datetime}' and time < '{end_datetime}'""",
                               midnight=True
                               )

        if data == {} or len(data.values()) == 0:
            temp_end_datetime = pd.to_datetime(self.end_date, utc=True) + timedelta(days=1)
            current_date = datetime.now(timezone.utc)
            if temp_end_datetime > current_date:
                raise ValueError('there is no bot_maintenance_data for given period')

        # print("get_bot_maintenance_data", data)
        active_bot = (data.get('inducted_bots', 0.0) or 0.0)
        inducted_bots = (data.get('inducted_bots', 0.0) or 0.0)
        Bots_in_maintenance = (data.get('Bots_in_maintenance', 0.0) or 0.0)
        self.utilfunction = CommonFunction()
        df_sheet = self.utilfunction.get_hardware_sheet_data()
        # df_sheet['Spare Bots'] = df_sheet['Spare Bots'].apply(lambda x: 0 if x=='' else int(x))
        row = df_sheet[df_sheet['SiteName used in Airflow'] == site]
        if not row.empty:
            row = row.reset_index()
            spare_bots = row['Spare Bots'][0]
            total_charger = row['Total Chargers'][0]

            if spare_bots == '' or str(spare_bots).isnumeric() == False:
                spare_bots = 0
            else:
                spare_bots = int(spare_bots)

            if total_charger == '' or str(total_charger).isnumeric() == False:
                total_charger = 0
            else:
                total_charger = int(total_charger)
        else:
            spare_bots = 0
            total_charger = data.get('inducted_chargers', 0.0) or 0.0

        if int(active_bot) < 0:
            active_bot = 0
        else:
            inducted_bots = inducted_bots - spare_bots
            active_bot = active_bot - spare_bots
            extra_maintenance_bots = Bots_in_maintenance - spare_bots
            if extra_maintenance_bots > 0:
                active_bot = active_bot - extra_maintenance_bots

        return {
            'Bots_in_maintenance': data.get('Bots_in_maintenance', 0.0) or 0.0,
            'total_chargers': total_charger,
            'active_bots': active_bot,
            'inducted_bots': inducted_bots
        }

    def get_order_lifecycle_alteryx(self):
        data = self.fetch_data("""
                select order_id, breached, breached_order_created, breached_order_complete
                from order_lifecycle_alteryx
                where time >= '{start_datetime}' and time < '{end_datetime}'""",
                               fetch_one=False
                               )
        return data

    def get_created_order_breached(self, **kwargs):

        query = """
            select count(order_id) as created_order_breached
            from order_events
            where time >= '{start_datetime}' and time < '{end_datetime}'
                and breach_state='breached'
                and event_name='order_created'
            group by time(1d, {hour})
        """
        data = self.fetch_data(query)
        return data

    def get_order_completed_breached(self, **kwargs):
        query = """
            select count(order_id) as order_completed_breached
            from order_events
            where time >= '{start_datetime}' and time < '{end_datetime}'
                and event_name = 'order_complete' 
                and breached='true'
            group by time(1d, {hour})
        """
        data = self.fetch_data(query)
        if data == {}:
            return {
                'order_completed_breached': 0
            }
        return data

    def get_orders_created_nonbreach(self, **kwargs):
        query = """
            select count(order_id) as orders_created_nonbreach
            from order_events
            where time >= '{start_datetime}' and time < '{end_datetime}'
                and event_name = 'order_created' 
                and breach_state='non_breach'
            group by time(1d, {hour})
        """
        data = self.fetch_data(query)
        if data == {}:
            return {
                'orders_created_nonbreach': 0
            }
        return data

    def get_pps_mode_type_value(self):
        data = self.fetch_data("""
                select pps_mode, type, value from pps_operator_log
                where time >= '{start_datetime}' and time < '{end_datetime}'""",
                               fetch_one=False
                               )
        return data

    def get_perface_put_operator_working_time(self, **kwargs):

        # query = """
        #     select
        #         mean(operator_working_time)/1000 as perface_put_operator_working_time,
        #         sum(operator_working_time)/1000 as SUM_perface_put_operator_working_time,
        #         count(operator_working_time) as COUNT_perface_put_operator_working_time
        #     from Alteryx..put_owt_summary
        #     where time >= '{start_datetime}' and time < '{end_datetime}'
        #       and operator_working_time<550000
        #     group by time(1d, {hour})
        # """
        # data = self.fetch_data(query, fetch_one=True)

        query = """
            select
                mean(value)/1000 as perface_put_operator_working_time,
                sum(value)/1000 as SUM_perface_put_operator_working_time,
                count(value) as COUNT_perface_put_operator_working_time
            from Alteryx..r2r_waittime_calculation
            where time >= '{start_datetime}' and time < '{end_datetime}'
            and event = 'rack_started_to_depart_pps' and task_type ='put' and value < 550000
            group by time(1d, {hour})
        """

        data = self.fetch_data(query, fetch_one=True)
        return data

    def get_perface_pick_operator_working_time(self, **kwargs):
        # query = """
        #     select
        #         mean(Actual_operator_working_time)/1000 as perface_pick_operator_working_time,
        #         sum(Actual_operator_working_time)/1000 as SUM_perface_pick_operator_working_time,
        #         count(Actual_operator_working_time) as COUNT_perface_pick_operator_working_time
        #     from Alteryx..operator_working_time_summary_airflow
        #     where time >= '{start_datetime}' and time < '{end_datetime}'
        #       and Actual_operator_working_time<550000
        #     group by time(1d, {hour})
        # """
        # data = self.fetch_data(query, fetch_one=True)

        query = """
            select
                mean(value)/1000 as perface_pick_operator_working_time,
                sum(value)/1000 as SUM_perface_pick_operator_working_time,
                count(value) as COUNT_perface_pick_operator_working_time
            from Alteryx..r2r_waittime_calculation
            where time >= '{start_datetime}' and time < '{end_datetime}'
             and event = 'rack_started_to_depart_pps' and task_type ='pick' and value < 550000
            group by time(1d, {hour})
        """
        data = self.fetch_data(query, fetch_one=True)

        if data == {}:
            return {
                'perface_pick_operator_working_time': 0,
                'SUM_perface_pick_operator_working_time': 0,
                'COUNT_perface_pick_operator_working_time': 0
            }
        return data

    def get_perface_pick_operator_working_time_for_ttp(self, **kwargs):
        query = """
            select
                mean(Actual_operator_working_time)/1000 as ttp_pick_operator_working_time,
                sum(Actual_operator_working_time)/1000 as SUM_ttp_pick_operator_working_time,
                count(Actual_operator_working_time) as COUNT_ttp_pick_operator_working_time
            from Alteryx..operator_working_time_summary_airflow
            where time >= '{start_datetime}' and time < '{end_datetime}'
              and Actual_operator_working_time<550000
            group by time(1d, {hour})
        """
        data = self.fetch_data(query, fetch_one=True)

        if data == {}:
            query = """
                select
                    mean(opertor_working_time)/1000 as ttp_pick_operator_working_time,
                    sum(opertor_working_time)/1000 as SUM_ttp_pick_operator_working_time,
                    count(opertor_working_time) as COUNT_ttp_pick_operator_working_time
                from Alteryx..operator_working_time_airflow
                where time >= '{start_datetime}' and time < '{end_datetime}'
                and opertor_working_time<300000
                group by time(1d, {hour})
            """
            data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            return {
                'ttp_pick_operator_working_time': 0,
                'SUM_ttp_pick_operator_working_time': 0,
                'COUNT_ttp_pick_operator_working_time': 0
            }
        return data

    def get_perface_pick_tote_to_tote_transaction_time_for_ttp(self, **kwargs):

        query = """
            select
                mean(time_diff_b_w_pptl_press_to_rack_arrival)/1000 as pick_tote_to_tote_transaction_time,
                sum(time_diff_b_w_pptl_press_to_rack_arrival)/1000 as SUM_pick_tote_to_tote_transaction_time
            from Alteryx..operator_working_time_summary_airflow
            where time >= '{start_datetime}' and time < '{end_datetime}'
            and time_diff_b_w_pptl_press_to_rack_arrival<550000
            group by time(1d, {hour})
        """
        data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            return {
                'pick_tote_to_tote_transaction_time': 0,
                'SUM_pick_tote_to_tote_transaction_time': 0
            }
        return data

    def get_perface_put_tote_to_tote_transaction_time_for_ttp(self, **kwargs):

        query = """
            select
                mean(operator_waiting_time)/1000 as put_tote_to_tote_transaction_time,
                sum(operator_waiting_time)/1000 as SUM_put_tote_to_tote_transaction_time
            from Alteryx..put_owt_summary
            where time >= '{start_datetime}' and time < '{end_datetime}'
            and operator_waiting_time<550000
            group by time(1d, {hour})
        """
        data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            query = """
                select
                    mean(tote_to_tote_time)/1000 as put_tote_to_tote_transaction_time,
                    sum(tote_to_tote_time)/1000 as SUM_put_tote_to_tote_transaction_time
                from Alteryx..put_operator_working_time
                where time >= '{start_datetime}' and time < '{end_datetime}'
                and tote_to_tote_time>0
                group by time(1d, {hour})
            """
            data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            return {
                'put_tote_to_tote_transaction_time': 0,
                'SUM_put_tote_to_tote_transaction_time': 0
            }
        return data

    def get_perface_pick_operator_waiting_time_for_ttp(self, **kwargs):

        query = """
            select
                mean(operator_waiting_time)/1000 as ttp_pick_operator_waiting_time,
                sum(operator_waiting_time)/1000 as SUM_ttp_pick_operator_waiting_time,
                count(operator_waiting_time) as COUNT_ttp_pick_operator_waiting_time
            from Alteryx..operator_working_time_summary_airflow
            where time >= '{start_datetime}' and time < '{end_datetime}'
            and operator_waiting_time<550000
            group by time(1d, {hour})
        """
        data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            return {
                'ttp_pick_operator_waiting_time': 0,
                'SUM_ttp_pick_operator_waiting_time': 0,
                'COUNT_ttp_pick_operator_waiting_time': 0
            }
        return data

    def get_perface_put_operator_working_time_for_ttp(self, **kwargs):

        query = """
            select
            mean(value)/1000 as ttp_put_operator_working_time,
            sum(value)/1000 as SUM_ttp_put_operator_working_time,
            count(value) as COUNT_ttp_put_operator_working_time
            from Alteryx..r2r_waittime_calculation
            where  task_type = 'put'  and event='rack_started_to_depart_pps' and value<550000 and
            time >= '{start_datetime}' and time < '{end_datetime}'
            group by time(1d, {hour})
        """
        data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            query = """
                select
                    mean(operator_working_time)/1000 as ttp_put_operator_working_time,
                    sum(operator_working_time)/1000 as SUM_ttp_put_operator_working_time,
                    count(operator_working_time) as COUNT_ttp_put_operator_working_time
                from Alteryx..put_owt_summary
                where time >= '{start_datetime}' and time < '{end_datetime}'
                and operator_working_time<550000
                group by time(1d, {hour})
            """
            data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            query = """
                select
                    mean(opertor_working_time)/1000 as ttp_put_operator_working_time,
                    sum(opertor_working_time)/1000 as SUM_ttp_put_operator_working_time,
                    count(opertor_working_time) as COUNT_ttp_put_operator_working_time
                from Alteryx..put_operator_working_time
                where time >= '{start_datetime}' and time < '{end_datetime}'
                and opertor_working_time<550000
                group by time(1d, {hour})
            """
            data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            return {
                'ttp_put_operator_working_time': 0,
                'SUM_ttp_put_operator_working_time': 0,
                'COUNT_ttp_put_operator_working_time': 0
            }
        return data

    def get_perface_put_operator_waiting_time_for_ttp(self, **kwargs):
        query = """
            select
            mean(value)/1000 as ttp_put_operator_waiting_time,
            sum(value)/1000 as SUM_ttp_put_operator_waiting_time,
            count(value) as COUNT_ttp_put_operator_waiting_time
            from Alteryx..r2r_waittime_calculation
            where  task_type = 'put'  and event='rack_arrived_to_pps' and value<550000 and
            time >= '{start_datetime}' and time < '{end_datetime}'
            group by time(1d, {hour})
        """
        data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            query = """
                select
                    mean(operator_waiting_time)/1000 as ttp_put_operator_waiting_time,
                    sum(operator_waiting_time)/1000 as SUM_ttp_put_operator_waiting_time,
                    count(operator_waiting_time) as COUNT_ttp_put_operator_waiting_time
                from Alteryx..put_owt_summary
                where time >= '{start_datetime}' and time < '{end_datetime}'
                and operator_waiting_time<550000
                group by time(1d, {hour})
            """
            data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            query = """
                   select
                       mean(tote_wait_time)/1000 as ttp_put_operator_waiting_time,
                       sum(tote_wait_time)/1000 as SUM_ttp_put_operator_waiting_time,
                       count(tote_wait_time) as COUNT_ttp_put_operator_waiting_time
                   from Alteryx..put_operator_working_time
                   where time >= '{start_datetime}' and time < '{end_datetime}'
                   and tote_wait_time<550000
                   group by time(1d, {hour})
               """
            data = self.fetch_data(query, fetch_one=True)
        if data == {}:
            return {
                'ttp_put_operator_waiting_time': 0,
                'SUM_ttp_put_operator_waiting_time': 0,
                'COUNT_tttp_put_operator_waiting_time': 0
            }
        return data

    def get_put_events_data(self):
        data = self.fetch_data("""
                select possible_amount, put_id, event_name from put_events
                where time >= '{start_datetime}' and time < '{end_datetime}'""",
                               fetch_one=False,
                               format="dataframe"
                               )

        if 'event_name' in data.columns:
            return {
                'new_put_request': len(data[data['event_name'] == 'put_created'].index),
                'items_to_be_put': len(data[data['event_name'] == 'put_calculated'].index),
            }
        return {
            'new_put_request': 0,
            'items_to_be_put': 0,
        }

    def get_last_inventory_data(self):
        df = self.fetch_data("""
                select category,inventory_qty from storage_metrics_v2
                where time >= '{start_datetime}' and time < '{end_datetime}' order by time desc limit 3 """,
                             fetch_one=False,
                             format="dataframe"
                             )
        inventor_a = inventor_b = inventor_c = 0
        if not df.empty:
            try:
                inventor_a = df[df['category'] == 'a']['inventory_qty'].iloc[0]
            except:
                inventor_a = 0

            try:
                inventor_b = df[df['category'] == 'b']['inventory_qty'].iloc[0]
            except:
                inventor_b = 0

            try:
                inventor_c = df[df['category'] == 'c']['inventory_qty'].iloc[0]
            except:
                inventor_c = 0

        return {'last_inventory_qty_for_category_a': inventor_a, 'last_inventory_qty_for_category_b': inventor_b,
                'last_inventory_qty_for_category_c': inventor_c}

    def get_last_sku_pareto_data(self):
        df = self.fetch_data("""
                   select category,sku_qty from sku_pareto
                   where time >= '{start_datetime}' and time < '{end_datetime}' order by time desc limit 3 """,
                             fetch_one=False,
                             format="dataframe"
                             )
        sku_qty_a = sku_qty_b = sku_qty_c = 0
        if not df.empty:
            try:
                sku_qty_a = df[df['category'] == 'a']['sku_qty'].iloc[0]
            except:
                sku_qty_a = 0

            try:
                sku_qty_b = df[df['category'] == 'b']['sku_qty'].iloc[0]
            except:
                sku_qty_b = 0

            try:
                sku_qty_c = df[df['category'] == 'c']['sku_qty'].iloc[0]
            except:
                sku_qty_c = 0

        return {'sku_qty_a': sku_qty_a, 'sku_qty_b': sku_qty_b,
                'sku_qty_c': sku_qty_c}

    def get_total_put_request(self):
        data = self.fetch_data("""
                select count(distinct(external_sr_id)) as total_put_request from item_put
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})
            """,
                               fetch_one=True,
                               )
        if data == {}:
            return {'total_put_request': 0}
        return data

    def get_average_line_count(self):
        data = self.fetch_data("""
                select mean(line_count) as avg_line_count from picks_per_rack_face
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})
            """,
                               fetch_one=True,
                               )
        if data == {}:
            return {'avg_line_count': 0}
        return data

    def get_quantity_per_orderline(self):
        data = self.fetch_data("""
                select mean(qty_per_orderline) as qty_per_orderline from system_order_stats
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})
            """,
                               fetch_one=True,
                               )
        if data == {}:
            return {'qty_per_orderline': 0}
        return data

    def get_active_chargers(self):
        data = self.fetch_data("""
                select charger_id, connectivity from charger_uptime
                where connectivity='connected' and charger_id!='-1' and
                    time >= '{start_datetime}' and time < '{end_datetime}'
            """,
                               fetch_one=False,
                               format="dataframe"
                               )
        if data.empty:
            return {'active_chargers': 0}

        return {
            'active_chargers': len(data['charger_id'].unique())
        }

    def get_srms_data(self):
        timez = self.site.get('timezone')
        q = f"""select COUNT(Case when container.state = 'back_warehouse_full' then stock_unit.quantity else 0 end ) as back_warehouse_full_count,
            ((service_request.updated_on :: timestamp at time zone 'UTC') at time zone '{timez}')::date,
            SUM(Case when container.state in ('physically_damaged','item_missing','unscannable','front_missing','front_unscannable','back_warehouse_full','complete') 
            then stock_unit.quantity else 0 end ) as items_to_be_put_srms,            
            SUM(Case when container.state='complete' then stock_unit.quantity else 0 end ) as items_put_srms 
            from service_request , service_request_actuals , container_stock_units , container ,  stock_unit 
            where  service_request.id = service_request_actuals.service_request_id and service_request_actuals.actuals_id = container.id 
            and service_request_actuals.actuals_id = container_stock_units.container_id 
            and container_stock_units.stockunits_id = stock_unit.id and service_request.type = 'PUT' and  
            service_request.updated_on>='{self.start_date}' and service_request.updated_on<'{self.end_date}' 
            group by ((service_request.updated_on :: timestamp at time zone 'UTC') at time zone '{timez}')::date"""

        print(f"{self.site.get('postgres_pf_user')}:{self.site.get('sslrootcert')}:{self.site.get('sslcert')}")
        print(
            f"{self.site.get('sslkey')}:{self.site.get('postgres_pf')}:{self.site.get('postgres_pf_port')}:{self.site.get('postgres_pf_password')}")
        if self.site.get('postgres_pf_user') != "":
            self.postgres_conn = postgres_connection(database='platform_srms',
                                                     user=self.site.get('postgres_pf_user'), \
                                                     sslrootcert=self.site.get('sslrootcert'), \
                                                     sslcert=self.site.get('sslcert'), \
                                                     sslkey=self.site.get('sslkey'),
                                                     host=self.site.get('postgres_pf'), \
                                                     port=self.site.get('postgres_pf_port'),
                                                     password=self.site.get('postgres_pf_password'),
                                                     dag_name=os.path.basename(__file__),
                                                     site_name=self.site.get('name'))
            try:
                data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
            except Exception as error:
                print("Error added delay")
                time.sleep(30)
                data = self.postgres_conn.fetch_postgres_data_in_chunk(q)
            self.postgres_conn.close()
            data = pd.DataFrame(data,
                                columns=['back_warehouse_full_count', 'date', 'items_to_be_put_srms', 'items_put_srms'])
            return {
                'back_warehouse_full_count': data['back_warehouse_full_count'].sum(),
                'items_to_be_put_srms': data['items_to_be_put_srms'].sum(),
                'items_put_srms': data['items_put_srms'].sum(),
                'put_exceptions_srms': data['items_to_be_put_srms'].sum() - data['items_put_srms'].sum(),
            }
        else:
            return {
                'back_warehouse_full_count': 0,
                'items_to_be_put_srms': 0,
                'items_put_srms': 0,
                'put_exceptions_srms': 0,
            }

    def get_open_pick_stations(self):
        df = self.fetch_data("""
                select * from "pps_data" 
                WHERE time >= '{start_datetime}' and time < '{end_datetime}' 
                and mode='pick' and front_logged_in='true' and status='open' """,
                             fetch_one=False, format="dataframe"
                             )
        if df.empty:
            return {'open_pick_stations': 0}
        open_pick_pps = df['pps_id'].nunique()
        return {'open_pick_stations': open_pick_pps}

    def get_open_put_stations(self):
        df = self.fetch_data("""
                select * from "pps_data" 
                WHERE time >= '{start_datetime}' and time < '{end_datetime}' 
                and mode='put' and front_logged_in='true' and status='open' """,
                             fetch_one=False, format="dataframe"
                             )
        if df.empty:
            return {'open_put_stations': 0}
        open_put_pps = df['pps_id'].nunique()
        return {'open_put_stations': open_put_pps}

    def get_uph(self, working_time, picks_per_face, rack_to_rack_time):
        total_owt = working_time['SUM_ttp_pick_operator_working_time']
        ppf = picks_per_face['picks_per_face']
        r2r = rack_to_rack_time['Avg_value']
        items_picked = picks_per_face['SUM_picks_per_face']
        uph = (3600 * ppf) / (r2r + ppf * (total_owt / items_picked)) if items_picked != 0 else 0
        return {'uph': uph}

    def get_hourly_ppf_item_picked(self):
        data = self.fetch_data(
            """ select
                    mean(total_picks_value) as picks_per_face,
                    sum(total_picks_value) as item_picked
                from picks_per_rack_face
                where
                    time >= '{start_datetime}' and time < '{end_datetime}'
                    group by time(1h)""",
            fetch_one=False, format="dataframe"
        )
        if data.empty:
            return pd.DataFrame(columns=['time', 'pps_id', 'picks_per_face', 'item_picked'])
        return data

    def get_hourly_working_time(self):
        data = self.fetch_data("""
                select sum(Actual_operator_working_time)/1000 as working_time from Alteryx..operator_working_time_summary_airflow
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1h)""",
                               fetch_one=False, format="dataframe"
                               )
        if data.empty:
            data = self.fetch_data("""
                    select mean(value)/1000 as working_time from Alteryx..r2r_waittime_calculation
                    where time >= '{start_datetime}' and time < '{end_datetime}' and event = 'rack_started_to_depart_pps' and task_type = 'pick'
                    group by time(1h)""",
                                   fetch_one=False, format="dataframe"
                                   )
        if data.empty:
            return pd.DataFrame(columns=['time', 'working_time'])
        return data

    def get_hourly_rack_to_rack_time(self):
        data = self.fetch_data("""
            select mean(time_diff_b_w_pptl_press_to_rack_arrival)/1000 as r2r_time from Alteryx..operator_working_time_summary_airflow
            where time >= '{start_datetime}' and time < '{end_datetime}'
            group by time(1h)""",
                               fetch_one=False, format="dataframe"
                               )
        if data.empty:
            data = self.fetch_data("""
                    select mean(value)/1000 as r2r_time from Alteryx..r2r_waittime_calculation
                    where time >= '{start_datetime}' and time < '{end_datetime}' and event = 'rack_arrived_to_pps' and task_type = 'pick'
                    group by time(1h)""",
                                   fetch_one=False, format="dataframe"
                                   )

        if data.empty:
            return pd.DataFrame(columns=['time', 'r2r_time'])
        return data

    def get_hourly_pps_open(self):
        df = self.fetch_data(""" select count(distinct pps_id) as pps_open from (
                select count(mode) as row_count from "pps_data" 
                WHERE time >= '{start_datetime}' and time < '{end_datetime}' 
                and mode='pick' and front_logged_in='true' and status='open' group by time(1h), pps_id) group by time(1h)""",
                             fetch_one=False, format="dataframe"
                             )
        if df.empty:
            return pd.DataFrame(columns=['time', 'pps_open'])
        return df

    def get_best_worst_warehouse_uph(self):
        hourly_working_time = self.get_hourly_working_time().reset_index()
        hourly_rack_to_rack_time = self.get_hourly_rack_to_rack_time().reset_index()
        hourly_ppf_item_picked = self.get_hourly_ppf_item_picked().reset_index()
        pps_open_per_hour = self.get_hourly_pps_open().reset_index()
        if hourly_working_time.empty or hourly_rack_to_rack_time.empty or hourly_ppf_item_picked.empty or pps_open_per_hour.empty:
            return {'best_warehouse_uph': 0, 'worst_warehouse_uph': 0, 'best_hour_pps_open': 0,
                    'worst_hour_pps_open': 0}
        df = pd.merge(hourly_working_time, hourly_rack_to_rack_time, on='index', how='outer')
        df = pd.merge(df, hourly_ppf_item_picked, on='index', how='outer')
        df = pd.merge(df, pps_open_per_hour, on='index', how='outer')
        df = df.fillna(0)
        df['uph'] = df.apply(lambda x: x['picks_per_face'] * 3600 / (
                    x['r2r_time'] + x['picks_per_face'] * (x['working_time'] / x['item_picked'])) if (
                    x['item_picked'] != 0 and x['r2r_time'] != 0) else 0, axis=1)
        df = df[df['uph'] != 0]
        best_warehouse_uph = df['uph'].max()
        worst_warehouse_uph = df['uph'].min()
        best_hour_pps_open = df[df['uph'] == best_warehouse_uph]['pps_open'].min()
        worst_hour_pps_open = df[df['uph'] == worst_warehouse_uph]['pps_open'].max()
        return {'best_warehouse_uph': best_warehouse_uph, 'worst_warehouse_uph': worst_warehouse_uph,
                'best_hour_pps_open': best_hour_pps_open, 'worst_hour_pps_open': worst_hour_pps_open}

    def get_hourly_pps_ppf_item_picked(self):
        df = self.fetch_data(
            """ select
                    mean(total_picks_value) as picks_per_face,
                    sum(total_picks_value) as item_picked
                from picks_per_rack_face
                where
                    time >= '{start_datetime}' and time < '{end_datetime}'
                    group by time(1h), pps_id fill(0)""",
            fetch_one=False, format="dataframe", get_all_data=True
        )
        if (hasattr(df, "empty") and df.empty) or (hasattr(df, "raw") and not df.raw.get("series")):
            return pd.DataFrame(columns=['time', 'pps_id', 'picks_per_face', 'item_picked'])

        result = pd.concat(
            [
                points.reset_index()
                .rename(columns={'index': 'time'})
                .assign(pps_id=tags[0][1])
                [['pps_id', 'time', 'picks_per_face', 'item_picked']]
                for (measurement, tags), points in df.items()
            ],
            ignore_index=True
        )
        return result

    def get_hourly_pps_working_time(self):
        df = self.fetch_data("""
                select sum(Actual_operator_working_time)/1000 as working_time from Alteryx..operator_working_time_summary_airflow
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1h), pps_id fill(0)""",
                             fetch_one=False, format="dataframe", get_all_data=True
                             )
        if (hasattr(df, "empty") and df.empty) or (hasattr(df, "raw") and not df.raw.get("series")):
            df = self.fetch_data("""
                    select mean(value)/1000 as working_time from Alteryx..r2r_waittime_calculation
                    where time >= '{start_datetime}' and time < '{end_datetime}' and event = 'rack_started_to_depart_pps' and task_type = 'pick'
                    group by time(1h), pps_id fill(0)""",
                                 fetch_one=False, format="dataframe", get_all_data=True
                                 )
        if (hasattr(df, "empty") and df.empty) or (hasattr(df, "raw") and not df.raw.get("series")):
            return pd.DataFrame(columns=['time', 'working_time', 'pps_id'])
        result = pd.concat(
            [
                points.reset_index()
                .rename(columns={'index': 'time'})
                .assign(pps_id=tags[0][1])
                [['pps_id', 'time', 'working_time']]
                for (measurement, tags), points in df.items()
            ],
            ignore_index=True
        )
        return result

    def get_hourly_pps_rack_to_rack_time(self):
        df = self.fetch_data("""
            select mean(time_diff_b_w_pptl_press_to_rack_arrival)/1000 as r2r_time from Alteryx..operator_working_time_summary_airflow
            where time >= '{start_datetime}' and time < '{end_datetime}' 
            group by time(1h), pps_id fill(0)""",
                             fetch_one=False, format="dataframe", get_all_data=True
                             )
        if (hasattr(df, "empty") and df.empty) or (hasattr(df, "raw") and not df.raw.get("series")):
            df = self.fetch_data("""
                    select mean(value)/1000 as r2r_time from Alteryx..r2r_waittime_calculation
                    where time >= '{start_datetime}' and time < '{end_datetime}' and event = 'rack_arrived_to_pps' and task_type = 'pick'
                    group by time(1h), pps_id fill(0)""",
                                 fetch_one=False, format="dataframe", get_all_data=True
                                 )

        if (hasattr(df, "empty") and df.empty) or (hasattr(df, "raw") and not df.raw.get("series")):
            return pd.DataFrame(columns=['time', 'r2r_time', 'pps_id'])
        result = pd.concat(
            [
                points.reset_index()
                .rename(columns={'index': 'time'})
                .assign(pps_id=tags[0][1])
                [['pps_id', 'time', 'r2r_time']]
                for (measurement, tags), points in df.items()
            ],
            ignore_index=True
        )
        return result

    def calculate_uph(self, x):
        uph = x['picks_per_face'] * 3600 / (
                    x['r2r_time'] + x['picks_per_face'] * (x['working_time'] / x['item_picked'])) if (
                    x['item_picked'] != 0 and x['r2r_time'] != 0) else 0
        return uph

    def get_best_pps_uph(self):
        hourly_working_time = self.get_hourly_pps_working_time()
        hourly_rack_to_rack_time = self.get_hourly_pps_rack_to_rack_time()
        hourly_ppf_item_picked = self.get_hourly_pps_ppf_item_picked()
        if hourly_working_time.empty or hourly_rack_to_rack_time.empty or hourly_ppf_item_picked.empty:
            return {'best_pps_uph': 0, 'best_pps': 0}

        df = pd.merge(hourly_working_time, hourly_rack_to_rack_time, on=['time', 'pps_id'], how='outer')
        df = pd.merge(df, hourly_ppf_item_picked, on=['time', 'pps_id'], how='outer')
        df = df.fillna(0)
        df['uph'] = df.apply(lambda x: self.calculate_uph(x) if x['item_picked'] != 0 else 0, axis=1)
        print("-------------------Started--------------------")
        best_pps_uph = df['uph'].max()
        best_pps = df[df['uph'] == best_pps_uph]['pps_id'].min()
        print("-------------------Ended--------------------")
        return {'best_pps_uph': best_pps_uph, 'best_pps': best_pps}

    def get_put_uph(self, working_time, put_per_rack_face, rack_to_rack_time):
        total_owt = working_time['SUM_ttp_put_operator_working_time']
        ppf = put_per_rack_face['put_per_face']
        r2r = rack_to_rack_time['ttp_put_operator_waiting_time']
        items_put = put_per_rack_face['SUM_put_per_face']
        put_uph = (3600 * ppf) / (r2r + ppf * (total_owt / items_put)) if (items_put != 0 and r2r != 0) else 0
        return {'put_uph': put_uph}

    def daily_site_kpi(self, **kwargs):
        self.CommonFunction = CommonFunction()
        daterange = self.get_datetime_interval2()
        for i in daterange.index:
            self.start_date = daterange['start_date'][i]
            self.end_date = daterange['end_date'][i]
            self.daily_site_kpi1(**kwargs)

    def daily_site_kpi_by_kpi(self, end_date, **kwargs):
        self.CommonFunction = CommonFunction()
        daterange = self.get_datetime_interval3(end_date)
        for i in daterange.index:
            self.start_date = daterange['start_date'][i]
            self.end_date = daterange['end_date'][i]
            print(f"daily site:{self.start_date}, end_date:{self.end_date}")
            self.daily_site_kpi1(**kwargs)

    def daily_site_kpi1(self, **kwargs):

        params = self.get_params()
        isvalid = self.check_influx_server()
        self.order_event_data = pd.DataFrame()
        if isvalid:
            order_completed = self.get_order_completed()
            agg_pps_operator_log = self.get_agg_pps_operator_log()
            mean_pps_operator = self.get_mean_pps_operator()
            time_to_pick_one_time = self.get_time_to_pick_one_time()
            picks_per_face = self.get_picks_per_face()
            pick_UPH_60min = self.get_pick_UPH_60min()
            count_butler = self.get_count_butler()
            put_UPH_60min = self.get_put_UPH_60min()
            put_per_face = self.get_put_per_face()
            put_items = self.get_put_items()
            put_per_face_agg = self.get_put_per_face_agg()
            warehouse_occupancy = self.get_warehouse_occupancy()
            total_put_exception_events = self.get_total_put_exception_events()
            pps_operator_agg = self.get_pps_operator_agg()
            total_rack_pr = self.get_total_rack_pr()
            total_put_expectations = self.get_total_put_expectations()
            time_to_put_one_item = self.get_time_to_put_one_item()
            order_tasked = self.get_order_tasked()
            order_created = self.get_order_created()
            bot_maintenance_data = self.get_bot_maintenance_data()
            perface_pick_operator_working_time_for_ttp = self.get_perface_pick_operator_working_time_for_ttp()
            perface_put_operator_working_time_for_ttp = self.get_perface_put_operator_working_time_for_ttp()

            perface_pick_operator_waiting_time_for_ttp = self.get_perface_pick_operator_waiting_time_for_ttp()
            perface_put_operator_waiting_time_for_ttp = self.get_perface_put_operator_waiting_time_for_ttp()

            perface_put_operator_working_time = self.get_perface_put_operator_working_time()
            perface_pick_operator_working_time = self.get_perface_pick_operator_working_time()

            perface_pick_tote_to_tote_transaction_time_for_ttp = self.get_perface_pick_tote_to_tote_transaction_time_for_ttp()
            perface_put_tote_to_tote_transaction_time_for_ttp = self.get_perface_put_tote_to_tote_transaction_time_for_ttp()

            created_order_breached = self.get_created_order_breached()
            order_completed_breached = self.get_order_completed_breached()
            orders_created_nonbreach = self.get_orders_created_nonbreach()
            put_events_data = self.get_put_events_data()
            total_put_request = self.get_total_put_request()
            average_line_count = self.get_average_line_count()
            quantity_per_orderline = self.get_quantity_per_orderline()
            active_chargers = self.get_active_chargers()
            order_created_at_risk = self.get_order_created_at_risk()
            order_tasked_at_risk = self.get_order_tasked_at_risk()
            order_tasked_breached = self.get_order_tasked_breached()
            postgres_srms = self.get_srms_data()
            site_working_hours = self.get_site_working_hours()
            port_entry_totes_presented = self.get_port_entry_totes_presented()
            port_exit_totes_processed = self.get_port_exit_totes_processed()
            port_entry_errors = self.get_port_entry_errors()
            port_exit_errors = self.get_port_exit_errors()
            last_inventory_data = self.get_last_inventory_data()
            last_sku_pareto_data = self.get_last_sku_pareto_data()

            pick_stations = self.get_open_pick_stations()
            put_stations = self.get_open_put_stations()
            uph = self.get_uph(perface_pick_operator_working_time_for_ttp, picks_per_face, agg_pps_operator_log)
            best_worst_warehouse_uph = self.get_best_worst_warehouse_uph()
            best_pps_uph = self.get_best_pps_uph()
            put_uph = self.get_put_uph(perface_put_operator_working_time_for_ttp, put_per_face,
                                       perface_put_operator_waiting_time_for_ttp)

            combined_data = {
                **order_completed,
                **agg_pps_operator_log,
                **put_per_face_agg,
                **warehouse_occupancy,
                **total_put_exception_events,
                **pps_operator_agg,
                **total_rack_pr,
                **total_put_expectations,
                **time_to_put_one_item,
                **order_tasked,
                **order_created,
                **bot_maintenance_data,
                **perface_put_operator_working_time,
                **perface_pick_operator_working_time,
                **perface_pick_operator_working_time_for_ttp,
                **perface_put_operator_working_time_for_ttp,
                **perface_pick_operator_waiting_time_for_ttp,
                **perface_put_operator_waiting_time_for_ttp,
                **perface_pick_tote_to_tote_transaction_time_for_ttp,
                **perface_put_tote_to_tote_transaction_time_for_ttp,
                **created_order_breached,
                **order_completed_breached,
                **orders_created_nonbreach,
                **put_events_data,
                **total_put_request,
                **average_line_count,
                **quantity_per_orderline,
                **active_chargers,
                **mean_pps_operator,
                **time_to_pick_one_time,
                **picks_per_face,
                **pick_UPH_60min,
                **count_butler,
                **put_UPH_60min,
                **put_per_face,
                **put_items,
                **order_created_at_risk,
                **order_tasked_at_risk,
                **order_tasked_breached,
                **postgres_srms,
                **site_working_hours,
                **port_entry_totes_presented,
                **port_exit_totes_processed,
                **port_entry_errors,
                **port_exit_errors,
                **last_inventory_data,
                **last_sku_pareto_data,
                **pick_stations,
                **put_stations,
                **uph,
                **best_worst_warehouse_uph,
                **best_pps_uph,
                **put_uph
            }

            combined_data.update({
                "UPH_RTR": ((combined_data['picks_per_face'] * 3600) / (combined_data['Avg_value'] + (
                            combined_data['picks_per_face'] * combined_data['time_to_pick_one_time'])) if not pd.isna(
                    combined_data['picks_per_face']) and combined_data['picks_per_face'] != 0 else 0),
                "UPH_RTR_99": ((combined_data['picks_per_face'] * 3600) / (combined_data['Avg_99'] + (
                            combined_data['picks_per_face'] * combined_data['time_to_pick_one_time'])) if not pd.isna(
                    combined_data['picks_per_face']) and combined_data['picks_per_face'] != 0 else 0),
                "operator_working_time_2": (combined_data['perface_pick_operator_working_time'] / combined_data[
                    'picks_per_face'] if not pd.isna(combined_data['picks_per_face']) and combined_data[
                    'picks_per_face'] != 0 and not pd.isna(combined_data['perface_pick_operator_working_time']) else 0),
                "UPH_RTR_95": ((combined_data['picks_per_face'] * 3600) / (combined_data['Avg_95'] + (
                            combined_data['picks_per_face'] * combined_data['time_to_pick_one_time'])) if not pd.isna(
                    combined_data['picks_per_face']) and combined_data['picks_per_face'] != 0 else 0),
                "UPH_RTR_90": ((combined_data['picks_per_face'] * 3600) / (combined_data['Avg_90'] + (
                            combined_data['picks_per_face'] * combined_data['time_to_pick_one_time'])) if not pd.isna(
                    combined_data['picks_per_face']) and combined_data['picks_per_face'] != 0 else 0),
                "RTR_avg": combined_data['Avg_value'],
                "RTR_avg_99": combined_data['Avg_99'],
                "RTR_avg_95": combined_data['Avg_95'],
                "RTR_avg_90": combined_data['Avg_90'],
                "UPH_15min": combined_data.get('mean_UPH_15min', 0),
                "UPH_30min": combined_data.get('mean_UPH_30min', 0),
                "UPH_60min": combined_data.get('mean_UPH_60min', 0),
                "operator_working_time": combined_data.get('time_to_pick_one_time', 0),
                "Total_Bots": combined_data["Count_butler"],
                "Order_breach_percent": (
                    ((combined_data['order_completed_breached'] * 100) / combined_data['order_completed']) if (
                                combined_data['order_completed'] != 0 and combined_data[
                            'order_completed_breached'] != 0) else 0)
            })

            #            if combined_data['picks_per_face'] == 0 and combined_data['time_to_pick_one_time'] == 0:
            #                return None

            selected_fields = [
                'operator_working_time',
                'picks_per_face',
                'RTR_avg',
                'RTR_avg_99',
                'RTR_avg_95',
                'RTR_avg_90',
                'UPH_15min',
                'UPH_30min',
                'UPH_60min',
                'Total_Bots',
                'put_UPH_60min',
                'put_per_face',
                'CBM',
                'tx_expectation',
                'total_item_put',
                'avg_utilization',
                'total_exceptions',
                'put_rack_to_rack',
                'avg_item_per_tx',
                'CBM_put_actual',
                'slots_used',
                'avg_utilization_percent',
                'total_warehouse_cap_CBM',
                'total_rack_pr',
                'total_put_expectations',
                'time_to_put_one_item_value',
                'order_tasked',
                'order_created',
                'order_completed',
                'order_created_at_risk',
                'order_tasked_at_risk',
                'order_tasked_breached',
                'order_completed_breached',
                'Bots_in_maintenance',
                'inducted_bots',
                'active_bots',
                'created_order_breached',
                'Order_breach_percent',
                'perface_put_operator_working_time',
                'orders_created_nonbreach',
                'total_chargers',
                'new_put_request',
                'total_put_request',
                'avg_line_count',
                'qty_per_orderline',
                'active_chargers',
                'items_to_be_put',
                'perface_pick_operator_working_time',
                'RTR_Sum',
                'RTR_Count',
                'SUM_put_rack_to_rack',
                'COUNT_put_rack_to_rack',
                'SUM_picks_per_face',
                'COUNT_picks_per_face',
                'picks_per_face_per_item',
                'SUM_put_per_face',
                'COUNT_put_per_face',
                'put_per_face_per_item',
                'SUM_UPH_60min',
                'COUNT_UPH_60min',
                'SUM_put_UPH_60min',
                'COUNT_put_UPH_60min',
                'SUM_perface_put_operator_working_time',
                'COUNT_perface_put_operator_working_time',
                'SUM_perface_pick_operator_working_time',
                'COUNT_perface_pick_operator_working_time',
                'ttp_pick_operator_working_time',
                'SUM_ttp_pick_operator_working_time',
                'COUNT_ttp_pick_operator_working_time',
                'ttp_put_operator_working_time',
                'SUM_ttp_put_operator_working_time',
                'COUNT_ttp_put_operator_working_time',
                'ttp_pick_operator_waiting_time',
                'SUM_ttp_pick_operator_waiting_time',
                'COUNT_ttp_pick_operator_waiting_time',
                'ttp_put_operator_waiting_time',
                'SUM_ttp_put_operator_waiting_time',
                'COUNT_tttp_put_operator_waiting_time',
                'pick_tote_to_tote_transaction_time',
                'SUM_pick_tote_to_tote_transaction_time',
                'put_tote_to_tote_transaction_time',
                'SUM_put_tote_to_tote_transaction_time',
                'UPH_RTR',
                'UPH_RTR_99',
                'operator_working_time_2',
                'UPH_RTR_95',
                'UPH_RTR_90',
                'back_warehouse_full_count',
                'items_put_srms',
                'items_to_be_put_srms',
                'order_remaining',
                'prom_operator_working_time',
                'prom_picks_per_face',
                'prom_RTR',
                'prom_UPH',
                'put_exceptions_srms',
                'slots',
                'site_working_hr',
                'port_entry_totes_presented',
                'port_exit_totes_presented',
                'port_entry_errors',
                'port_exit_errors',
                'last_inventory_qty_for_category_a',
                'last_inventory_qty_for_category_b',
                'last_inventory_qty_for_category_c',
                'sku_qty_a',
                'sku_qty_b',
                'sku_qty_c',
                'open_pick_stations',
                'open_put_stations',
                'uph',
                'best_warehouse_uph',
                'worst_warehouse_uph',
                'best_hour_pps_open',
                'worst_hour_pps_open',
                'best_pps_uph',
                'best_pps',
                'put_uph'
            ]

            final_result = {
                key: round(float(combined_data.get(key, 0) or 0), 2)
                for key in selected_fields
            }

            final_df = pd.DataFrame.from_records([final_result])
            params = self.get_params()
            # final_df.at[0, 'time'] = pd.DatetimeIndex(
            #     [datetime.strftime(datetime.fromtimestamp(
            #         combined_data['time']/1000), INFLUX_DATETIME_FORMAT
            #     )])[0]
            final_df.at[0, 'time'] = pd.DatetimeIndex(
                [datetime.strptime(params.get('start_datetime'), INFLUX_DATETIME_FORMAT)])[0]

            final_df = final_df.set_index('time')
            final_df['site'] = [self.site.get('name')]

            # print("final_df", json.loads(final_df.to_json(orient='records')))
            # print("final_df index", final_df.index)
            write_influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'),
                                                                         self.site.get('central_influx'))
            isvalid = write_influx.is_influx_reachable(dag_name=os.path.basename(__file__),
                                                       site_name=self.site.get('name'))
            if isvalid:
                success = write_influx.write_all(final_df, 'daily_site_kpi_ayx', dag_name=os.path.basename(__file__),
                                                 site_name=self.site.get('name'), tag_columns=["site"])

            self.write_client = Write_InfluxData(host=self.site.get("write_influx_ip"),
                                                 port=self.site.get("write_influx_port"))
            self.write_client.writepoints(final_df, "daily_site_kpi_ayx",
                                          db_name=self.site.get("alteryx_out_db_name"),
                                          tag_columns=['site'],
                                          dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
        return

# MultiTenantDAG(
#     dag,
#     [
#         'daily_site_kpi',
#     ],
#     [],
#     DailySiteKPI
# ).create()
