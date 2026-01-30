import pandas as pd
import json
import gspread

import os
import airflow
from airflow import DAG
from utils.CommonFunction import SingleTenantDAGBase, MultiTenantDAG, average,CommonFunction ,Write_InfluxData
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone


from config import (
    INFLUX_DATETIME_FORMAT,
    GOOGLE_AUTH_JSON
)


dag = DAG(
    "all_site_kpi",
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval='6 7 */1 * *',
    default_args=CommonFunction().get_default_args_for_dag(),
    catchup=False,
)


def map_site_name_to_excel_name(site_name):
    if 'Colombia' in site_name:
        return 'Sodimac'
    if 'Loreal' in site_name:
        return 'Loreal'
    if 'Disney' in site_name:
        return 'Disney'
    if 'Adidas' in site_name:
        return 'Adidas'
    if 'Jysk' in site_name:
        return 'JYSK'
    if 'SBS' in site_name:
        return 'SBS_store'
    if 'Saitama' in site_name:
        return 'Trusco_Saitama'
    if 'Tohoku' in site_name:
        return 'Trusco_Tohoku'
    if 'Trusco (Ground' in site_name:
        return 'Trusco_Tohoku'
    if 'XPO-H&M' in site_name:
        return 'XPO_HnM'
    if 'XPO H&M' in site_name:
        return 'XPO_HnM'
    if 'Project One - Retail' in site_name:
        return 'Project_one'
    if 'Project' in site_name:
        return 'Project_one'
    if 'Clarios' in site_name:
        return 'JCI'
    if 'Sodimac Click & Collect' in site_name:
        return 'Sodimac_CnC'
    if 'Sodimac C&C' in site_name:
        return 'Sodimac_CnC'
    if 'C&C' in site_name:
        return 'Sodimac_CnC'
    if 'Ikea' in site_name:
        return 'IKEA'
    if 'Nippon' in site_name:
        return 'Nippon_Konpo'
    if 'GoldBond' in site_name:
        return 'Goldbond'
    if 'Apple' in site_name:
        return 'XPO_Apple'
    if 'Florida' in site_name:
        return 'Sodimac_CnC_florida'
    if 'sam' in site_name:
        return 'Sams_Club'
    return site_name


class AllSiteKPI():

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        print(f"generate_data: for this range:{start_date}:{end_date}")
        print(df)
        return df

    def get_datetime_interval2(self):
        influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        start_datetime = influx.fetch_one("""select * from all_site_kpi_new_ayx 
                      where site='{site}' order by time desc limit 1""".format(
            site=self.site.get('name')))
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
            start_datetime = start_datetime + timedelta(days=1)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=1)
        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        start_datetime =start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = datetime.now(timezone.utc)

        timeseriesdata = self.create_time_series_data2(start_datetime, end_datetime, '1d')
        return timeseriesdata

    def get_datetime_interval3(self,end_date):
        influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        start_datetime = influx.fetch_one("""select * from all_site_kpi_new_ayx 
                      where site='{site}' order by time desc limit 1""".format(
            site=self.site.get('name')))
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=3)
        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        start_datetime =start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = end_date

        timeseriesdata = self.create_time_series_data2(start_datetime, end_datetime, '1d')
        return timeseriesdata

    def get_datetime_interval(self, sheet_startdate, interval, midnight=None, after_12_check=False):
        sheet_datetime = datetime.strptime(sheet_startdate, INFLUX_DATETIME_FORMAT)
        # now = datetime.now()
        # end_datetime = datetime.strptime("{}-{}-{} {}:{}:00".format(
        #     now.year, now.month, now.day, sheet_datetime.hour, sheet_datetime.minute
        # ), "%Y-%m-%d %H:%M:%S")
        # influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        # start_datetime = influx.fetch_one("""select * from all_site_kpi_new_ayx
        #        where site='{site}' and time<'2023-03-23T07:00:00Z' order by time desc limit 1""".format(
        #     site=self.site.get('name')))
        start_datetime = pd.to_datetime(self.start_date, utc=True)
        end_datetime = pd.to_datetime(self.end_date, utc=True)
        hour, minute, second = map(str, self.site.get('starttime').split(':'))
        hour=hour+"h"+ minute+"m"
        # start_datetime = end_datetime - timedelta(days=int(interval/(24 * 60 * 60)))

        if after_12_check:
            start_datetime = start_datetime + timedelta(days=1) if start_datetime.hour > 12 else start_datetime
            end_datetime = end_datetime + timedelta(days=1) if end_datetime.hour > 12 else end_datetime

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
            return """ and pps_id!='8' and pps_id!='9' and pps_id!='10' and pps_id!='11' 
                and pps_id!='12' and pps_id!='13' and pps_id!='14' and pps_id!='15'
                and pps_id!='16' and pps_id!='17' and pps_id!='18' and pps_id!='19'
                and pps_id!='20'"""
        return ""

    def get_params(self, midnight=None, after_12_check=False):
        start_datetime, end_datetime, hour = self.get_datetime_interval(
            '2020-04-01 07:00:00',
            86400,
            midnight,
            after_12_check
        )
        return {
            'start_datetime': start_datetime,
            'end_datetime': end_datetime,
            'hour': hour,
            'extra_where': self.get_extra_where(),
        }

    def fetch_data(self, query, fetch_one=True, midnight=None, format="json", after_12_check=False):
        influx = self.CommonFunction.get_site_read_influx_client(site=self.site,database='GreyOrange')

        formatted_query = query.format(**self.get_params(midnight, after_12_check))
        if fetch_one:
            return influx.fetch_one(formatted_query, format=format)
        return influx.fetch_all(formatted_query, format=format)

    def fetch_data_by_alteryx(self, query, fetch_one=False, midnight=None, format="json", after_12_check=False):
        influx = self.CommonFunction.get_site_read_influx_client(site=self.site,database='Alteryx')

        formatted_query = query.format(**self.get_params(midnight, after_12_check))
        if fetch_one:
            return influx.fetch_one(formatted_query, format=format)
        return influx.fetch_all(formatted_query, format=format)

    def get_breached_and_completed_orders(self):  # Done
        order_data_df = self.fetch_data("""
                select * from order_lifecycle_alteryx
                where time >= '{start_datetime}' and time < '{end_datetime}'
            """,
                                        fetch_one=False,
                                        format="dataframe"
                                        )

        orders_completed_count = self.fetch_data("""
                select count(*) from order_lifecycle_alteryx
                where time >= '{start_datetime}' and time < '{end_datetime}'
                group by time(1d, {hour})""").get('count_PBT')


        if not order_data_df.empty and 'breached' in order_data_df.columns:
            order_data_df = order_data_df[order_data_df['breached'] == 'true']
            order_data_df= order_data_df.reset_index()
            order_breached_completed_count = len(order_data_df.index)
        else:
            order_breached_completed_count = orders_completed_count

        # if total_order_data == 0:
        #     query = """select count(PBT) as order_breached_completed from order_lifecycle_alteryx
        #         where time >= '{start_datetime}' and time < '{end_datetime}'
        #         group by time(1d, {hour})"""
        #     order_breached_completed_count = self.fetch_data(query).get('order_breached_completed', 0)
        # else:
        #     query = """select breached from order_lifecycle_alteryx
        #             where breached = 'true' and time >= '{start_datetime}' and
        #                 time < '{end_datetime}'"""
        #     order_breached_completed_count = self.fetch_data(query,fetch_one=False,format='dataframe').get('order_breached_completed', 0)
        # order_breached_completed_count = self.fetch_data(query).get('order_breached_completed', 0)


        breached_orders = order_breached_completed_count / orders_completed_count if orders_completed_count else 0

        return {
            'orders_completed': orders_completed_count,
            'order_breached_completed': order_breached_completed_count,
            'breached_orders': breached_orders
        }

    def get_mean_UPH(self):  # Done
        interval_througput_data = self.fetch_data("""
            select mean(*) from interval_throughput
            where time >= '{start_datetime}' and time < '{end_datetime}'
                and UPH_60min > 0 {extra_where} group by time(1d, {hour})
        """)
        if interval_througput_data == {}:
            return {
                'UPH': 0,
                'mean_UPH_15min_value': 0,
                'mean_UPH_30min_value': 0,
                'mean_UPH_60min_value': 0,
            }
        return {
            'UPH': interval_througput_data.get('mean_UPH_60min', 0),
            'mean_UPH_15min_value': interval_througput_data.get('mean_UPH_15min_value', 0),
            'mean_UPH_30min_value': interval_througput_data.get('mean_UPH_30min_value', 0),
            'mean_UPH_60min_value': interval_througput_data.get('mean_UPH_60min_value', 0),

        }

    def get_item_put(self):  # Done
        data = self.fetch_data("""
            select uom_quantity_int as qty from item_put
            where time >= '{start_datetime}' and time < '{end_datetime}' {extra_where}
        """, fetch_one=False, format="dataframe")

        if data.empty:
            return {'entity_put': 0}
        return {
            'entity_put': sum(data['qty'].tolist())
        }


    def get_item_picked(self):  # Done
        data = self.fetch_data("""select count(item_id)  as item_cnt from item_picked
                    where time >= '{start_datetime}' and time < '{end_datetime}' {extra_where}
                """, fetch_one=False, format="dataframe")
        if data.empty:
            return {'item_picked': 0}
        item_cnt =data['item_cnt'][0]

        offset_val = 0
        data_limit = 5000
        while offset_val < item_cnt:
            qry="""select * from item_picked
                        where time >= '{start_datetime}' and time < '{end_datetime}' {extra_where} order by time desc limit %s offset %s 
                    """ %(data_limit, offset_val)
            temp_data = self.fetch_data(qry, fetch_one=False, format="dataframe")
            if offset_val == 0:
                data = temp_data
            else:
                data = pd.concat([data, temp_data])
            offset_val = offset_val + data_limit

        data['qty']= 0

        if 'uom_quantity' not in data:
            data['uom_quantity']= data['value']

        if 'uom_quantity_int' not in data:
            data['uom_quantity_int']=data['uom_quantity']

        data['qty'] = data.apply(self.CommonFunction.calc_uom_quanity, axis=1)
        return {
            'item_picked': sum(data['qty'].tolist())
        }

    def get_agg_butler_uptime_ayx(self):  # Done
        butler_uptime_ayx_df = self.fetch_data_by_alteryx("""select * from butler_uptime_ayx
            where time >= '{start_datetime}' and time < '{end_datetime}'
        """, midnight=True, after_12_check=True, format='dataframe')

        if butler_uptime_ayx_df.empty:
            temp_end_datetime = pd.to_datetime(self.end_date, utc=True) + timedelta(days=1)
            current_date = datetime.now(timezone.utc)
            if temp_end_datetime > current_date:
                raise ValueError('There is no butler_uptime data for given period')
            else:
                return {}

        butler_uptime_ayx_df['time'] = butler_uptime_ayx_df.index

        print("Grouped ===", butler_uptime_ayx_df.groupby(
            ["time"], as_index=False
        ).apply(
            lambda df: pd.Series({'Concat_flag': ",".join(df['flag'].tolist())})
        ))

        butler_uptime_ayx_flagged_df = butler_uptime_ayx_df.merge(
            butler_uptime_ayx_df.groupby(
                ["time"], as_index=False
            ).apply(
                lambda df: pd.Series({'Concat_flag': ",".join(df['flag'].tolist())})
            ),
            how='inner',
            left_on='time',
            right_on='time'
        )

        butler_uptime_ayx_flagged_df = butler_uptime_ayx_flagged_df[
            butler_uptime_ayx_flagged_df['Concat_flag'].str.contains('Y')
        ]

        agg_butler_uptime_ayx_df = butler_uptime_ayx_flagged_df.groupby(
            ["time"], as_index=False
        ).apply(lambda df: pd.Series({
            'charging_time': sum(df['charging_time'].tolist()),
            'idle_time': sum(df['idle_time'].tolist()),
            'move_task': sum(df['move_task'].tolist()),
            'pps_task_time': sum(df['pps_task_time'].tolist()),
            'total_kpi': sum(df['total_kpi'].tolist()),
            'total_time': sum(df['total_time'].tolist()),
        }))
        if agg_butler_uptime_ayx_df.empty:
            return {}

        agg_butler_uptime_ayx_df['butler_uptime_hourly'] = agg_butler_uptime_ayx_df.apply(
            lambda row: round(
                (
                        row['pps_task_time'] + row['charging_time'] +
                        row['idle_time'] + row['move_task']
                ) / row['total_kpi'],
                2) if row['total_kpi'] != 0 else 0,
            axis=1
        )
        agg_butler_uptime_ayx_df = agg_butler_uptime_ayx_df[
            agg_butler_uptime_ayx_df['butler_uptime_hourly'] > 0.2
            ]
        if agg_butler_uptime_ayx_df.empty:
            return {}
        final_butler_uptime_ayx_df = agg_butler_uptime_ayx_df.groupby(
            [True] * len(agg_butler_uptime_ayx_df.index), as_index=False
        ).apply(lambda df: pd.Series({
            'Sum_charging_time': sum(df['charging_time'].tolist()),
            'Sum_idle_time': sum(df['idle_time'].tolist()),
            'Sum_move_task': sum(df['move_task'].tolist()),
            'Sum_pps_task_time':    sum(df['pps_task_time'].tolist()),
            'Sum_total_kpi': sum(df['total_kpi'].tolist()),
            'Sum_total_time': sum(df['total_time'].tolist()),
        }))

        final_butler_uptime_ayx_df['butler_uptime_hourly'] = final_butler_uptime_ayx_df.apply(
            lambda row: round(
                (
                        row['Sum_pps_task_time'] + row['Sum_charging_time'] +
                        row['Sum_idle_time'] + row['Sum_move_task']
                ) / row['Sum_total_kpi'],
                2),
            axis=1
        )
        final_butler_uptime_ayx_df = final_butler_uptime_ayx_df[
            final_butler_uptime_ayx_df['butler_uptime_hourly'] > 0.20]
        final_butler_uptime_ayx_df['butler_uptime'] = final_butler_uptime_ayx_df.apply(
            lambda row: round(
                (
                        row['Sum_pps_task_time'] + row['Sum_charging_time'] +
                        row['Sum_idle_time'] + row['Sum_move_task']
                ) / row['Sum_total_kpi'],
                2),
            axis=1
        )
        return json.loads(final_butler_uptime_ayx_df.to_json(orient='records'))[0]

    def get_agg_charger_uptime_ayx(self):  # Done
        charger_uptime_ayx_df = self.fetch_data_by_alteryx("""select * from charger_uptime_ayx
            where time >= '{start_datetime}' and time < '{end_datetime}'
        """, midnight=True, after_12_check=True, format='dataframe')

        if charger_uptime_ayx_df.empty:
            temp_end_datetime = pd.to_datetime(self.end_date, utc=True) + timedelta(days=1)
            current_date = datetime.now(timezone.utc)
            if temp_end_datetime > current_date:
                raise ValueError('There is no Charger uptime data for given period')
            else:
                return {}

        charger_uptime_ayx_df['time'] = charger_uptime_ayx_df.index

        charger_uptime_ayx_flagged_df = charger_uptime_ayx_df.merge(
            charger_uptime_ayx_df.groupby(
                ["time"], as_index=False
            ).apply(
                lambda df: pd.Series({'Concat_flag': ",".join(df['flag'].tolist())})
            ),
            how='inner',
            left_on='time',
            right_on='time'
        )

        charger_uptime_ayx_flagged_df = charger_uptime_ayx_flagged_df[
            charger_uptime_ayx_flagged_df['Concat_flag'].str.contains('Y')
        ]

        agg_charger_uptime_ayx_df = charger_uptime_ayx_flagged_df.groupby(
            ["time"], as_index=False
        ).apply(lambda df: pd.Series({
            'online_time': sum(df['online_time'].tolist()),
            # 'idle_time': sum(df['idle_time'].tolist()),
            # 'move_task': sum(df['move_task'].tolist()),
            # 'pps_task_time': sum(df['pps_task_time'].tolist()),
            # 'total_kpi': sum(df['total_kpi'].tolist()),
            'total_time': sum(df['total_time'].tolist()),
        }))
        if agg_charger_uptime_ayx_df.empty:
            return {}
        agg_charger_uptime_ayx_df['charger_uptime_hourly'] = agg_charger_uptime_ayx_df.apply(
            lambda row: round(
                (
                    row['online_time']
                ) / row['total_time'],
                2) if row['total_time'] != 0 else 0,
            axis=1
        )
        agg_charger_uptime_ayx_df = agg_charger_uptime_ayx_df[
            agg_charger_uptime_ayx_df['charger_uptime_hourly'] > 0.2
            ]

        final_charger_uptime_ayx_df = agg_charger_uptime_ayx_df.groupby(
            [True] * len(agg_charger_uptime_ayx_df.index), as_index=False
        ).apply(lambda df: pd.Series({
            'Sum_online_time': sum(df['online_time'].tolist()),
            'Sum_total_time': sum(df['total_time'].tolist()),
        }))
        if final_charger_uptime_ayx_df.empty:
            return {}

        final_charger_uptime_ayx_df['charger_uptime'] = final_charger_uptime_ayx_df.apply(
            lambda row: round(
                (
                    row['Sum_online_time']
                ) / row['Sum_total_time'],
                2),
            axis=1
        )

        return json.loads(final_charger_uptime_ayx_df.to_json(orient='records'))[0]

    def get_put_mean_UPH(self):  # Done
        data = self.fetch_data("""
            select mean(*) from put_interval_throughput
            where time >= '{start_datetime}' and time < '{end_datetime}' {extra_where} 
        """)

        return {
            'put_UPH': data.get('mean_UPH_60min', 0),
            'mean_UPH_15min_value': data.get('mean_UPH_15min_value', 0),
            'mean_UPH_30min_value': data.get('mean_UPH_30min_value', 0),
            'mean_UPH_60min_value': data.get('mean_UPH_60min_value', 0)
        }

    def get_count_pps_data(self):
        q = """select 5*count(cnt) from 
        (select count(status) as cnt from pps_data 
        where time >= '{start_datetime}' and time < '{end_datetime}' and mode = 'pick' and status = 'open' and front_logged_in = 'true' 
        group by time(5m)) where cnt>0
        """
        pick_entries = self.fetch_data(q, fetch_one=True)

        q = """ select 5*count(cnt) from 
        (select count(status) as cnt from pps_data 
        where time >= '{start_datetime}' and time < '{end_datetime}' and mode = 'put' and status = 'open' and front_logged_in = 'true' 
        group by time(5m)) where cnt>0 """
        put_entries = self.fetch_data(q, fetch_one=True)        

        if not pick_entries:
            operator_minutes_pick = 0
        else:
            operator_minutes_pick = pick_entries['count']
        if not put_entries:
            operator_minutes_put = 0
        else:
            operator_minutes_put  = put_entries['count']

        uom_per_opmin = 0
        return {
            'operator_minutes_pick': operator_minutes_pick,
            'operator_minutes_put': operator_minutes_put,
            'uom_per_opmin': uom_per_opmin
        }

    def get_count_pps_operator_log(self):
        data = self.fetch_data("""
                select type,value,pps_mode from pps_operator_log
                where time >= '{start_datetime}' and time < '{end_datetime}'
            """,
                               fetch_one=False,
                               format="dataframe"
                               )

        if data.empty or 'pps_mode' not in data.columns:
            return {
                'Total_rack_presentation_pick': 0,
                'Total_rack_presentation_put': 0
            }
        pick_data = data.loc[(data["pps_mode"] == 'pick_mode') & (data["type"] == 'working_time')]
        put_data = data.loc[(data["pps_mode"] == 'put_mode') & (data["type"] == 'working_time')]
        pick_data_count = pick_data.shape[0]
        put_data_count = put_data.shape[0]

        return {
            'Total_rack_presentation_pick': pick_data_count,
            'Total_rack_presentation_put': put_data_count
        }

    def get_wh_manager(self):  # Done
        df_sheet = self.CommonFunction.get_Warehouse_Manager_sheet()
        df_sheet = df_sheet[(df_sheet['excel_names'] == self.site.get('name'))]
        geo = ""
        if not df_sheet.empty:
            df_sheet = df_sheet.reset_index()
            geo = df_sheet['Geo'][0]

        return {
            'geography': geo,
            'ops_hours': "07:00"
        }

    def get_ops_support_NPS(self):  # Done
        influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        data = influx.fetch_one("""
            select ops_support_NPS from nps_ops_support_ayx
            where site='{site}' and time >= '{start_datetime}' and time < '{end_datetime}'
        """.format(
            site=self.site.get('name'),
            **self.get_params()
        ), format="json")

        return {
            'ops_support_NPS': data.get('ops_support_NPS')
        }

    def get_operator_NPS(self):  # Done
        influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        data = influx.fetch_one("""
            select * from nps_operator_ayx
            where site='{site}' and time >= '{start_datetime}' and time < '{end_datetime}'
        """.format(
            site=self.site.get('name'),
            **self.get_params()
        ), format="json")

        return {
            'operator_NPS_audit': data.get('nps_operator_audit'),
            'operator_NPS_pick': data.get('nps_operator_pick'),
            'operator_NPS_put': data.get('nps_operator_put'),
        }

    def get_latency_data(self):  # Done
        influx = self.CommonFunction.get_site_read_influx_client(site=self.site,database="Alteryx")
        result = influx.client.query("show measurements")

        screen_trigger_time_table_exits = len(list(
            filter(lambda row: row.get('name') == 'screen_trigger_time', list(result)[0])
        )) == 1

        if not screen_trigger_time_table_exits:
            mean_value = self.fetch_data("""
                    select mean(value)/1000 as mean
                    from screen_trigger_time 
                    where time >= '{start_datetime}' and time < '{end_datetime}'
                        and last_trigger!='undefined' and trigger_source!='screen_reupdate'
                        and trigger_source!='trigger_update'
                    group by time(1d, {hour})
                """)

            total_count = self.fetch_data("""
                    select count(value) as total_count
                    from screen_trigger_time 
                    where time >= '{start_datetime}' and time < '{end_datetime}'
                        and last_trigger!='undefined' and trigger_source!='screen_reupdate'
                        and trigger_source!='trigger_update'
                    group by time(1d, {hour})
                """)

            count_lessthan500 = self.fetch_data("""
                    select count(value) as count_lessthan500
                    from screen_trigger_time 
                    where time >= '{start_datetime}' and time < '{end_datetime}' and (value/1000)<500
                        and last_trigger!='undefined' and trigger_source!='screen_reupdate'
                        and trigger_source!='trigger_update'
                    group by time(1d, {hour})
                """)
        else:
            mean_value = self.fetch_data("""
                    select mean(UPH_60min)/1000 as mean
                    from put_interval_throughput 
                    where time >= '{start_datetime}' and time < '{end_datetime}'
                    and UPH>0
                    group by time(1d, {hour})
                """)

            total_count = self.fetch_data("""
                    select count(UPH_60min) as total_count
                    from put_interval_throughput 
                    where time >= '{start_datetime}' and time < '{end_datetime}'
                    and UPH>0
                    group by time(1d, {hour})
                """)

            count_lessthan500 = self.fetch_data("""
                    select count(UPH_60min) as count_lessthan500
                    from put_interval_throughput 
                    where time >= '{start_datetime}' and time < '{end_datetime}'
                    and UPH>0
                    group by time(1d, {hour})
                """)

        return {
            "latency_avg_ms": mean_value.get('mean'),
            "total_latency_count": total_count.get('total_count'),
            "latency_count_lessthan_500": count_lessthan500.get('count_lessthan500'),
        }

    def get_barcode_agg_data(self):
        alteryx_influx = self.CommonFunction.get_site_read_influx_client(site=self.site,database='Alteryx')
        params = self.get_params()
        startdate, enddate = (
            params.get('start_datetime').split(' ')[0],
            params.get('end_datetime').split(' ')[0]
        )
        print(f"startdate:{startdate},enddate:{enddate}")
        result = alteryx_influx.query("""
            select * from barcode_travelled
            where time >= '{startdate}' and time<'{enddate}'
        """.format(startdate=startdate, enddate=enddate))
        result_len= len(result.values())
        if result_len > 0:
            result = list(result.values())[0]
            barcode_data = json.loads(result.to_json(orient='records'))
        else:
            barcode_data = None

        if result_len == 0:
            raise ValueError('There is no barcode data for given period')

        if barcode_data:
            barcode_travelled = barcode_data[0].get("barcode")
        else:
            barcode_travelled = 0

        if self.site.get('name') in ['Sodimac_CnC_florida', 'Sodimac_C', 'Sodimac', 'sodimac_cnc7', 'sodimac_cnc3']:
            butler_nav_incidents_query = """
                select
                    start_time, Is_auto_resolved, time_difference,
                    barcode, error_name, butler_id, L2, L1
                from butler_nav_incidents
                where time >= '{start_datetime}' and time < '{end_datetime}'
            """
        else:
            butler_nav_incidents_query = """
                select
                    start_time, Is_auto_resolved, time_difference,
                    barcode, error_name, butler_id, L2, L1
                from butler_nav_incidents
                where time >= '{start_datetime}' and time < '{end_datetime}' and zone != 'defzone'
            """

        butler_nav_incidents_df = self.fetch_data(butler_nav_incidents_query, fetch_one=False, format='dataframe')
        if butler_nav_incidents_df.empty:
            return {
                "total_errors_excl_hardemg": 0,
                "barcode_per_stop": 0,
                "barcode_travelled": 0,
                "total_errors": 0,
            }

        butler_nav_incidents_df = butler_nav_incidents_df.loc[
            (butler_nav_incidents_df["Is_auto_resolved"] == "No") &
            (butler_nav_incidents_df["time_difference"] > 10)
            ]

        butler_nav_incidents_df['time'] = butler_nav_incidents_df.index
        butler_nav_incidents_df['time_old'] = butler_nav_incidents_df['time']

        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        if butler_nav_incidents_df.empty:
            return {
                "total_errors_excl_hardemg": 0,
                "barcode_per_stop": 0,
                "barcode_travelled": barcode_travelled,
                "total_errors": 0,
            }

        butler_nav_incidents_df["date_start"] = butler_nav_incidents_df.apply(
            lambda row: row["time_old"].replace(hour=hour, minute=minute, second=0, microsecond=0), axis=1
        )
        butler_nav_incidents_df["date_end"] = butler_nav_incidents_df.apply(
            lambda row: row["date_start"] + timedelta(days=1), axis=1
        )
        butler_nav_incidents_df["date_minus"] = butler_nav_incidents_df.apply(
            lambda row: row["date_start"] - timedelta(days=1), axis=1
        )
        butler_nav_incidents_df["time"] = butler_nav_incidents_df.apply(
            lambda row: row["date_start"] if row["time_old"] >= row["date_start"] and row["time_old"] <= row[
                "date_end"] else (row["date_minus"] if row["time_old"] <= row["date_start"] and row["time_old"] >= row[
                "date_minus"] else None),
            axis=1
        )

        butler_nav_incidents_df = butler_nav_incidents_df.sort_values(
            by=["time", "butler_id", "time_old"], axis=0
        )

        butler_nav_incidents_df["new_field"] = 0
        butler_nav_incidents_df["new_field2"] = 0
        butler_nav_incidents_df["new_field3"] = 0
        butler_nav_incidents_list = json.loads(butler_nav_incidents_df.to_json(orient='records'))

        for i in range(1, len(butler_nav_incidents_list)):
            row = butler_nav_incidents_list[i]
            prev_row = butler_nav_incidents_list[i - 1]
            if row["time"] == prev_row["time"] and row["barcode"] == prev_row["barcode"] \
                    and row["butler_id"] == prev_row["butler_id"] and row["L2"] == prev_row["L2"] \
                    and row["error_name"] == prev_row["error_name"]:
                row["new_field"] = prev_row["new_field"]
            else:
                row["new_field"] = prev_row["new_field"] + 1

        for i in range(1, len(butler_nav_incidents_list)):
            row = butler_nav_incidents_list[i]
            prev_row = butler_nav_incidents_list[i - 1]
            if row["new_field"] == prev_row["new_field"]:
                row["new_field2"] = row["time_old"] - prev_row["time_old"]
            else:
                row["new_field2"] = 0

        for i in range(1, len(butler_nav_incidents_list)):
            row = butler_nav_incidents_list[i]
            prev_row = butler_nav_incidents_list[i - 1]
            if row["new_field"] == prev_row["new_field"] and row["new_field2"] < 600:
                row["new_field3"] = prev_row["new_field3"]
            else:
                row["new_field3"] = prev_row["new_field3"] + 1

        butler_nav_incidents_df = pd.DataFrame.from_records(butler_nav_incidents_list)

        total_errors = len(pd.unique(butler_nav_incidents_df["new_field3"]))

        butler_nav_incidents_df = butler_nav_incidents_df[
            (butler_nav_incidents_df["L1"] == "Hard Emg error") |
            (butler_nav_incidents_df["L1"] == "Server Safety Stop")
            ]

        total_errors_excl_hardemg = len(pd.unique(butler_nav_incidents_df["new_field3"]))
        barcode_per_stop = barcode_travelled / total_errors

        return {
            "total_errors_excl_hardemg": total_errors_excl_hardemg,
            "barcode_per_stop": barcode_per_stop,
            "barcode_travelled": barcode_travelled,
            "total_errors": total_errors,
        }

    def get_partners(self):
        gc = gspread.service_account(GOOGLE_AUTH_JSON)
        sheet = gc.open_by_key("1BXMBCv7FN7-C1tHATkpvFUUlb-fOf_dr6VQj3ul-zsw")
        worksheet = sheet.worksheet('site_mappings')
        return worksheet.get_all_values()

    def check_influx_server(self):
        influx = self.CommonFunction.get_site_read_influx_client(site=self.site,database='GreyOrange')
        valid = influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
        return valid

    def all_site_kpi(self, **kwargs):
        self.CommonFunction = CommonFunction()
        daterange = self.get_datetime_interval2()
        for i in daterange.index:
            self.start_date = daterange['start_date'][i]
            self.end_date = daterange['end_date'][i]
            self.all_site_kpi1(**kwargs)

    def all_site_kpi_by_kpi(self,end_date,**kwargs):
        self.CommonFunction = CommonFunction()
        daterange = self.get_datetime_interval3(end_date)
        for i in daterange.index:
            self.start_date = daterange['start_date'][i]
            self.end_date = daterange['end_date'][i]
            print(f"all site:{self.start_date}, end_date:{self.end_date}")
            self.all_site_kpi1(**kwargs)

    def all_site_kpi1(self, **kwargs):
        self.CommonFunction= CommonFunction()
        params = self.get_params()
        isvalid = self.check_influx_server()
        if isvalid:
            functions = [
                'get_breached_and_completed_orders',
                'get_mean_UPH',
                'get_item_put',
                'get_item_picked',
                'get_agg_butler_uptime_ayx',
                'get_agg_charger_uptime_ayx',
                'get_put_mean_UPH',
                'get_ops_support_NPS',
                'get_operator_NPS',
                'get_latency_data',
                'get_barcode_agg_data',
                'get_count_pps_operator_log',
                'get_count_pps_data'
            ]

            combined_data = {}

            for func_name in functions:
                func = getattr(self, func_name)
                combined_data.update(**func())

            combined_data['operator_NPS_audit'] = combined_data.get('operator_NPS_audit', 0.5) or 0.5
            combined_data['operator_NPS_pick'] = combined_data.get('operator_NPS_pick', 0.5) or 0.5
            combined_data['operator_NPS_put'] = combined_data.get('operator_NPS_put', 0.5) or 0.5
            combined_data['WH_Score'] = combined_data.get('WH_Score', -1) or -1
            combined_data['ops_support_NPS'] = combined_data.get('ops_support_NPS', 0.5) or 0.5
            combined_data['sw_uptime'] = combined_data.get('sw_uptime', 1) or 1
            combined_data['breached_orders'] = combined_data.get('breached_orders', 0) or 0
            combined_data['copy_flag'] = 1 if combined_data.get('item_picked') in (0, None) else 0
            combined_data['entity_picked'] = combined_data.get('item_picked')
            combined_data['order_breached_completed'] = (
                        combined_data.get('order_breached_completed', 0) or 0) if self.site.get(
                'name') == 'Coupang' else (combined_data.get('order_breached_completed', 0) or 0)
            combined_data['order_completed'] = (combined_data.get('order_completed', 0) or 0) if self.site.get(
                'name') == 'Coupang' else (combined_data.get('order_completed', 0) or 0)
            combined_data['ops_hours'] = combined_data.get('ops_hours', 0) or 0
            combined_data['breached_olpn_orders'] = combined_data.get('breached_olpn_orders', 0) or 0
            combined_data['total_orders'] = combined_data.get('total_orders', 0) or 0
            combined_data['total_breached_orders'] = combined_data.get('total_breached_orders', 0) or 0
            combined_data['Total_rack_presentation_pick'] = combined_data.get('Total_rack_presentation_pick', 0) or 0
            combined_data['Total_rack_presentation_put'] = combined_data.get('Total_rack_presentation_put', 0) or 0
            combined_data['uom_per_opmin'] = (combined_data.get('item_picked') / (
                        combined_data.get('operator_minutes_pick', 0) + combined_data.get('operator_minutes_put',
                                                                                          0)) if (combined_data.get(
                'operator_minutes_pick', 0) + combined_data.get('operator_minutes_put', 0)) != 0 else 0)
            # combined_data['nps_operator_pick'] = combined_data.get('nps_operator_pick', 0) or 0
            # combined_data['nps_operator_put'] = combined_data.get('nps_operator_put', 0) or 0
            combined_data['Downtime'] = combined_data.get('Downtime', 0) or 0
            combined_data['breached_orders'] = (combined_data.get('breached_olpn_orders', 0) or 0) if self.site.get(
                'name') == 'Coupang' else (combined_data.get('breached_orders', 0) or 0)

            selected_fields = [
                'Downtime',
                'NPS_ops_support',
                'Sum_charging_time',
                'Sum_idle_time',
                'Sum_online_time',
                'Sum_pps_task_time',
                'Sum_total_kpi',
                'Sum_total_time',
                'Total_rack_presentation_pick',
                'Total_rack_presentation_put',
                'UPH',
                'WH_Score',
                'barcode_per_stop',
                'barcode_travelled',
                'breached_orders',
                'butler_uptime',
                'charger_uptime',
                'copy_flag',
                'entity_picked',
                'entity_put',
                'geography',
                'latency_avg_ms',
                'latency_count_lessthan_500',
                # 'nps_operator_audit',
                # 'nps_operator_pick',
                # 'nps_operator_put',
                'operator_NPS_audit',
                'operator_NPS_pick',
                'operator_NPS_put',
                'operator_minutes_pick',
                'operator_minutes_put',
                'ops_hours',
                'ops_support_NPS',
                'order_breached_completed',
                'orders_completed',
                'partner',
                'put_UPH',
                'score',
                'sw_uptime',
                'total_errors',
                'total_errors_excl_hardemg',
                'total_latency_count',
                'uom_per_opmin'
            ]

            final_result = {}
            for key in selected_fields:
                print(key, combined_data.get(key, 0))
                final_result[key] = round(float(combined_data.get(key, 0) or 0), 2)

            params = self.get_params()

            final_df = pd.DataFrame.from_records([final_result])

            final_df.at[0, 'time'] = pd.DatetimeIndex(
                [datetime.strptime(params.get('start_datetime'), INFLUX_DATETIME_FORMAT)])[0]
            # final_df['time'] = pd.to_datetime(final_df['time'])
            final_df = final_df.set_index('time')
            final_df['site'] = [self.site.get('name')]

            excel_partner_name = map_site_name_to_excel_name(self.site.get('name'))

            partners_data = self.get_partners()
            partners_df = pd.DataFrame(partners_data[1:], columns=partners_data[0])

            partner_df = partners_df[partners_df["excel_names"] == excel_partner_name]

            if partner_df.empty:
                partner = self.site.get('name')
            else:
                partner = list(partner_df["Unit/Partner"])[0]

            final_df['partner'] = [partner]
            final_df['geography'] = final_df['geography'].astype(str)

            #            print("final_df", json.loads(final_df.to_json(orient='records')))
            #            print("final_df index", final_df.index)
            write_influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'),
                                                          self.site.get('central_influx'))
            isvalid = write_influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
            if isvalid:
                success = write_influx.write_all(final_df, 'all_site_kpi_new_ayx', dag_name=os.path.basename(__file__), site_name=self.site.get('name'), tag_columns=["site"])

            self.write_client = Write_InfluxData(host=self.site.get("write_influx_ip"),
                                                 port=self.site.get("write_influx_port"))
            self.write_client.writepoints(final_df, "all_site_kpi_new_ayx",
                                          db_name=self.site.get("alteryx_out_db_name"),
                                          tag_columns=['site'],
                                          dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
# MultiTenantDAG(
#     dag,
#     ['all_site_kpi'],
#     [],
#     AllSiteKPI
# ).create()
#
