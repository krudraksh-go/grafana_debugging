import pandas as pd
import json

import gspread
import os

import airflow
from airflow import DAG
from airflow.exceptions import AirflowTaskTimeout
from utils.CommonFunction import SingleTenantDAGBase, MultiTenantDAG, CommonFunction
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone
from config import (
    INFLUX_DATETIME_FORMAT,
    GOOGLE_AUTH_JSON,
    PPS_STRATEGY_AYX_SHEET_ID
)


####################### Config #######################

dag = DAG(
    "pps_strategy_ayx",
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval="0 0 * * *",
    default_args=CommonFunction().get_default_args_for_dag(),
    catchup=False,
)

####################### Helper functions #######################

def fetch_section(row):
    for section_name in [
        'sector1',
        'sector2',
        'sector3',
        'sector4',
        'sector5',
        'sector6',
        'sector7',
    ]:
        if section_name in row['order_flow_name'].lower():
            return section_name
    return 'NA'

def check_if_pps_strategy_followed(row):
    if row['Priority'] == 'p1':
        if row['pps_id_in_strategy'] < row['no_of_pps_in_priority'] and \
            int(row['next_pps_id_in_strategy']) > 0:
            return False
        else:
            return True
    else:
        if row['pps_id_in_strategy'] > 0 and \
            int(row['previous_pps_id_in_strategy']) != row['previous_no_of_pps_in_priority']:
            return False
        else:
            return True


####################### Task definations #######################
class PpsStrategyAYX(SingleTenantDAGBase):

    def create_time_series_data2(self, start_date, end_date, interval):
        start_date1 = pd.date_range(start_date, end_date, freq=interval, closed='left')
        end_date1 = pd.date_range(start_date, end_date, freq=interval, closed='right')
        df = start_date1.to_frame(index=False)
        df["end_date"] = end_date1.to_frame(index=False)
        df.rename(columns={0: 'start_date'}, inplace=True)
        df = df[df["end_date"] == df["end_date"]]
        return df
    def get_datetime_interval2(self):
        client = self.get_site_read_influx_client('GreyOrange')

        query = "select * from pps_strategy_ayx order by time desc limit 1"
        start_datetime = client.fetch_one(query)
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=1)

        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = datetime.now(timezone.utc)
        timeseriesdata = self.create_time_series_data2(start_datetime,end_datetime, '1d')
        return timeseriesdata

    def pps_strategy_ayx(self, **kwargs):
        try:
            daterange = self.get_datetime_interval2()
            for i in daterange.index:
                self.start_date=daterange['start_date'][i]
                self.end_date=daterange['end_date'][i]
                self.pps_strategy_ayx1()
        except AirflowTaskTimeout as timeout_exception:
            raise timeout_exception                 
        except Exception as e:
            print(f"error:{e}")
            raise e

    def get_last_pps_strategy_data(self):
        client = self.get_site_read_influx_client('GreyOrange')

        query = "select * from pps_strategy_ayx order by time desc limit 1"
        data = client.fetch_one(query, format="json")
        return data

    def get_item_picked_data(self, **kwargs):
        # last_pps_strategy_data = self.get_last_pps_strategy_data()
        # end_datetime = datetime.utcnow().replace(second=0, minute=0)
        # time = datetime.fromtimestamp(
        #     last_pps_strategy_data.get('time') / 1000
        # )
        # print("TIme", time)
        # start_datetime = end_datetime - timedelta(seconds=3600)
        #
        # start_datetime = time if time < start_datetime else start_datetime

        query = "select * from item_picked where time >= '{start_datetime}' " \
                "and time < '{end_datetime}' order by time desc".format(
                    start_datetime=self.start_date.strftime(INFLUX_DATETIME_FORMAT),
                    end_datetime=self.end_date.strftime(INFLUX_DATETIME_FORMAT),
                )
        print(query)
        client = self.get_site_read_influx_client('GreyOrange')
        df = client.fetch_all(query, format="dataframe")
        if not df.empty:
            df["value"] = df.apply(
                lambda row: row["value"] or row["uom_quantity_int"],
                axis=1,
            )
            df = df[df["value"] == 1]
            df = df[["pps_id", "order_flow_name"]]
            df['time'] = df.index
            # df["order_flow_name"] = df.apply(fetch_section, axis=1)
            df["start_date"] = df["time"].map(
                lambda t: t.replace(minute=0, second=0).strftime(INFLUX_DATETIME_FORMAT)
            )
            df["end_date"] = df["time"].map(
                lambda t: t.replace(minute=59, second=59).strftime(INFLUX_DATETIME_FORMAT)
            )
            #print("df 3", json.loads(df.to_json(orient='records')))
            df = df.drop_duplicates(subset=['order_flow_name', 'pps_id', 'start_date'])
            df = df[df["order_flow_name"] != 'NA']
            #print("df 4", df)
            return json.loads(df.to_json(orient='records'))
        else:
            return {}

    def get_pps_strategy_from_sheet(self):
        gc = gspread.service_account(GOOGLE_AUTH_JSON)
        sheet = gc.open_by_key(PPS_STRATEGY_AYX_SHEET_ID)
        worksheet = sheet.worksheet(self.site.get('name'))
        return worksheet.get_all_values()

    def createdummydataframe(self):
        hour = self.end_date.strftime('%Y-%m-%dT%H:00:00')
        data = {'time': [self.end_date], 'followed_pps_strategy': [''],
                'following_strategy': [''], 'hour': [hour],
                'order_flow_name': [''], 'p1': [''], 'p2': [''], 'p3': ['']
                }
        final_result = pd.DataFrame(data)
        final_result = final_result.set_index('time')
        return final_result
    def pps_strategy_ayx1(self):
        item_picked_data = self.get_item_picked_data()
        pps_strategy_from_sheet = self.get_pps_strategy_from_sheet()

        #print("item_picked_data", item_picked_data)
        #print("pps_strategy_from_sheet", pps_strategy_from_sheet)

        item_picked_df = pd.DataFrame.from_records(item_picked_data)
        if not item_picked_df.empty:
            pps_strategy_df = pd.DataFrame(pps_strategy_from_sheet[1:], columns = pps_strategy_from_sheet[0])
            # pps_strategy_df['sector'] = pps_strategy_df.apply(lambda row: row['sector'].lower(), axis=1)
            melted_pps_strategy_df = pps_strategy_df.melt(id_vars=['sector'], var_name='Priority', value_name='strategy')



            joined_strategy_df = pd.merge(
                item_picked_df,
                melted_pps_strategy_df,
                how='inner',
                left_on='order_flow_name',
                right_on='sector'
            )[['order_flow_name', 'pps_id', 'start_date', 'Priority', 'strategy']]
            if not joined_strategy_df.empty:
                joined_strategy_df['pps_id_in_strategy'] = joined_strategy_df.apply(
                    lambda row: 1 if row['pps_id'] in row['strategy'].split(',') else 0,
                    axis=1
                )

                agg_pps_id_in_strategy = joined_strategy_df[
                    ["order_flow_name", "start_date", "Priority", "pps_id_in_strategy"]
                ].groupby(["order_flow_name", "start_date", "Priority"], as_index=False).sum()

                agg_no_of_pps_in_priority = melted_pps_strategy_df[['sector', 'Priority']]
                agg_no_of_pps_in_priority['no_of_pps_in_priority'] = melted_pps_strategy_df.apply(
                    lambda row: len(set(row['strategy'])),
                    axis=1
                )
                agg_no_of_pps_in_priority = agg_no_of_pps_in_priority[agg_no_of_pps_in_priority['no_of_pps_in_priority'] > 0]

                ordered_pps_priority_data = pd.merge(
                    agg_pps_id_in_strategy,
                    agg_no_of_pps_in_priority,
                    how='inner',
                    left_on=['order_flow_name', 'Priority'],
                    right_on=['sector', 'Priority']
                ).sort_values(by=["start_date", "order_flow_name", "Priority"], axis=0)
                ordered_pps_priority_data['next_pps_id_in_strategy'] = ordered_pps_priority_data['pps_id_in_strategy'].shift(-1)
                ordered_pps_priority_data['previous_pps_id_in_strategy'] = ordered_pps_priority_data['pps_id_in_strategy'].shift(1)
                ordered_pps_priority_data['previous_no_of_pps_in_priority'] = ordered_pps_priority_data['no_of_pps_in_priority'].shift(1)
                ordered_pps_priority_data['check'] = ordered_pps_priority_data.apply(
                    check_if_pps_strategy_followed,
                    axis=1
                )

                check_strategy_followed_1 = ordered_pps_priority_data[
                    ['start_date', 'order_flow_name', 'check']
                ].groupby(
                    ['start_date', 'order_flow_name'],
                    as_index=False
                ).apply(lambda df: pd.Series({'final1': all(df['check'].tolist())}))

                total_pps_opened_per_sector = item_picked_df[['start_date', 'order_flow_name', 'pps_id']].groupby(
                    ['start_date', 'order_flow_name'],
                    as_index=False
                ).apply(lambda df: pd.Series({'opened_pps':  ",".join(df['pps_id'].tolist())}))
                total_pps_opened_per_sector['total_pps_opened_per_sector'] = total_pps_opened_per_sector.apply(
                    lambda row: len(row['opened_pps'].split(',')),
                    axis=1
                )
                total_pps_following_strategy = ordered_pps_priority_data[
                    ['start_date', 'order_flow_name', 'pps_id_in_strategy']
                ].groupby(['start_date', 'order_flow_name'], as_index=False).apply(
                    lambda df: pd.Series({
                        'total_pps_following_strategy_per_sector':  sum(df['pps_id_in_strategy'].tolist())
                    })
                )

                check_strategy_followed_2 = total_pps_following_strategy.merge(
                    total_pps_opened_per_sector,
                    how='inner',
                    left_on=['start_date', 'order_flow_name'],
                    right_on=['start_date', 'order_flow_name']
                )
                check_strategy_followed_2['final2'] = check_strategy_followed_2.apply(
                    lambda row: False if row['total_pps_following_strategy_per_sector'] != row['total_pps_opened_per_sector'] else True,
                    axis=1
                )

                check_strategy_followed_combined = pd.merge(
                    check_strategy_followed_1,
                    check_strategy_followed_2,
                    how='inner',
                    left_on=['start_date', 'order_flow_name'],
                    right_on=['start_date', 'order_flow_name']
                )
                check_strategy_followed_combined['following_strategy'] = check_strategy_followed_combined.apply(
                    lambda row: True if row['final1'] is True and row['final2'] is True else False,
                    axis=1
                )

                final_followed_strategy = check_strategy_followed_combined[
                    ['start_date', 'order_flow_name', 'opened_pps', 'following_strategy']
                ]
                final_followed_strategy.rename(columns = {'opened_pps':'followed_pps_strategy'}, inplace = True)

                final_result = pd.merge(
                    final_followed_strategy,
                    pps_strategy_df,
                    how='inner',
                    left_on='order_flow_name',
                    right_on='sector',
                ).sort_values(by=["start_date"], axis=0)

                final_result = final_result.reset_index(drop=True)
                final_result['following_strategy'] = final_result.apply(
                    lambda row: 'T' if row['following_strategy'] else 'F',
                    axis=1
                )

                final_result.at[0, 'time'] = pd.DatetimeIndex(
                    [final_result.at[0, 'start_date']]
                )[0]

                for i in range(1, len(final_result.index)):
                    row = final_result.iloc[i]
                    previous_row = final_result.iloc[i-1]
                    if previous_row['start_date'] == row['start_date']:
                        final_result.at[i, 'time'] = final_result.at[i-1, 'time'] + timedelta(milliseconds=0.1)
                    else:
                        final_result.at[i, 'time'] = pd.DatetimeIndex([final_result.at[i, 'start_date']])[0]

                final_result = final_result.set_index('time')
                final_result = final_result.drop(
                    columns=['sector', 'start_date']
                )
                print("final_result", json.loads(final_result.to_json(orient='records')))

            else:
                final_result = self.createdummydataframe()
        else:
            final_result=self.createdummydataframe()

        write_influx = self.get_site_write_influx_client(self.site.get('out_db_name'))
        success = write_influx.write_all(final_result, 'pps_strategy_ayx', dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
        return


MultiTenantDAG(
    dag,
    ['pps_strategy_ayx'],
    [],
    PpsStrategyAYX
).create()
