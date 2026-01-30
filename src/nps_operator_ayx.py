import pandas as pd
import json

import gspread
import pytz
import numpy as np
from math import ceil

import os
import airflow
from airflow import DAG
from utils.CommonFunction import SingleTenantDAGBase, MultiTenantDAG, CommonFunction
from urllib.parse import urlparse
from datetime import datetime, timedelta

from config import (
    INFLUX_DATETIME_FORMAT,
)

####################### Config #######################


dag = DAG(
    "nps_operator_ayx",
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval="0 0 */1 * *",
    default_args=CommonFunction().get_default_args_for_dag(),
    catchup=False,
)

utc=pytz.UTC

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
class NpsOperatorAYX(SingleTenantDAGBase):
    def get_quarter_info(self):
        today = datetime.now()
        print("site data", self.site)
        hour, minute, second = map(int, self.site.get('starttime').split(':'))
        today = today.replace(hour=hour, minute=minute, second=0, microsecond=0)
        quarter = f'Q{ceil(today.month/3)}'

        if quarter == 'Q1':
            quarter_start_date = f'{today.year}-01-01'
        elif quarter == 'Q2':
            quarter_start_date = f'{today.year}-04-01'
        elif quarter == 'Q3':
            quarter_start_date = f'{today.year}-07-01'
        else:
            quarter_start_date = f'{today.year}-10-01'

        return {
            'quarter': quarter,
            'hour': hour,
            'minute': minute,
            'today': today.strftime(INFLUX_DATETIME_FORMAT),
            'quarter_start_date': quarter_start_date,
            'yesterday': (today - timedelta(days=1)).strftime(INFLUX_DATETIME_FORMAT),
        }

    def nps_ops_support_ayx(self):
        quarter_info = self.get_quarter_info()
        quarter_start_date = datetime.strptime(
            f"{quarter_info.get('quarter_start_date')} {quarter_info.get('hour')}:{quarter_info.get('minute')}:00",
            INFLUX_DATETIME_FORMAT
        )

        influx = self.get_central_influx_client('Alteryx', self.site.get('central_influx'))
        isvalid= influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        last_ops_support_nps = influx.fetch_one("""select * from ops_support_nps 
            where Site='{site}' order by time desc limit 1""".format(site=self.site.get('name')))

        if not last_ops_support_nps.empty and last_ops_support_nps.index[0] < utc.localize(quarter_start_date):
            query = "select * from ops_support_nps where Site='{site}' order by time desc limit 1"
        else:
            query = "select * from ops_support_nps where Site='{site}' and " \
                "time >= '{quarter_start_date}' and time <= '{today}'"

        formatted_query = query.format(
            site=self.site.get('name'),
            **quarter_info
        )
        scale_data_df = influx.fetch_all(formatted_query)

        print("scale_data_df", scale_data_df)

        if scale_data_df.empty:
            return

        scale_data_df['label'] = scale_data_df.apply(lambda row: get_label(int(row['Scale'] or '0')), axis=1)
        
        scale_data_count_df = scale_data_df.groupby(["User ID"], as_index=False).apply(
            lambda df: pd.Series({
                'Count_P':  len(list(filter(lambda label: label == 'P', df['label'].tolist()))),
                'Count_D':  len(list(filter(lambda label: label == 'D', df['label'].tolist()))),
                'Count_N':  len(list(filter(lambda label: label == 'N', df['label'].tolist()))),
                'Count_Nothing':  len(list(filter(lambda label: label == 'Nothing', df['label'].tolist()))),
            })
        )

        scale_data_count_df['Total'] = scale_data_count_df.apply(
            lambda row: (row['Count_P'] or 0) + (row['Count_D'] or 0) + (row['Count_N'] or 0),
            axis=1
        )
        scale_data_count_df['percent_P'] = scale_data_count_df.apply(
            lambda row: (row['Count_P'] or 0)/row['Total'],
            axis=1
        )
        scale_data_count_df['percent_D'] = scale_data_count_df.apply(
            lambda row: (row['Count_D'] or 0)/row['Total'],
            axis=1
        )
        scale_data_count_df['percent_N'] = scale_data_count_df.apply(
            lambda row: (row['Count_N'] or 0)/row['Total'],
            axis=1
        )
        scale_data_count_df['label'] = scale_data_count_df.apply(get_agg_label, axis=1)
        scale_data_agg_df = scale_data_count_df.groupby(
            ['label'], as_index=False
        ).apply(lambda df: pd.Series({'Count': len(df.index)}))
        transposed_scale_agg_data = scale_data_agg_df.pivot(columns='label')['Count']

        for col in  set(['Promoter', 'Detractor', 'Neutral', 'Nothing']) - set(transposed_scale_agg_data.columns):
            transposed_scale_agg_data = transposed_scale_agg_data.assign(**{col:0})

        print("before transposed_scale_agg_data", transposed_scale_agg_data)

        transposed_scale_agg_data['NPS_ops_support'] = transposed_scale_agg_data.apply(
            lambda row: round(
                (row.get('Promoter') - row.get('Detractor') ) / 
                (row.get('Detractor') + row.get('Promoter') + row.get('Neutral')),
                2
            ),
            axis=1
        )

        transposed_scale_agg_data['NPS_ops_support'] = transposed_scale_agg_data.apply(
            lambda row: 0.5 if not row['NPS_ops_support'] else row['NPS_ops_support'], axis=1)


        if last_ops_support_nps.index[0] < utc.localize(quarter_start_date):
            transposed_scale_agg_data['Detractor'] = 0.0
            transposed_scale_agg_data['Promoter'] = 0.0
            transposed_scale_agg_data['Neutral'] = 0.0
            transposed_scale_agg_data['NPS_ops_support'] = 0.5

        transposed_scale_agg_data = transposed_scale_agg_data.drop(columns=['Nothing'])

        transposed_scale_agg_data.at[0, 'time'] = pd.DatetimeIndex(
            [quarter_info.get('yesterday')]
        )[0]

        transposed_scale_agg_data = transposed_scale_agg_data.set_index('time')
        transposed_scale_agg_data = transposed_scale_agg_data.apply(pd.to_numeric)
        transposed_scale_agg_data['site'] = [self.site.get('name')]

        print("transposed_scale_agg_data", transposed_scale_agg_data)
        print("nps_ops_support_ayx", json.loads(transposed_scale_agg_data.to_json(orient='records')))

        write_influx = self.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        success = write_influx.write_all(transposed_scale_agg_data, 'nps_ops_support_ayx', dag_name=os.path.basename(__file__), site_name=self.site.get('name'), tag_columns=["site"])
        return


    def nps_operator_ayx(self):
        quarter_info = self.get_quarter_info()
        quarter_start_date = datetime.strptime(
            f"{quarter_info.get('quarter_start_date')} {quarter_info.get('hour')}:{quarter_info.get('minute')}:00",
            INFLUX_DATETIME_FORMAT
        )

        influx = self.get_site_read_influx_client('GreyOrange')
        isvalid= influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        query = "select * from session_feedback_data order by time desc limit 1"
        last_session_feedback_data = influx.fetch_one(query)
        if last_session_feedback_data.empty:
            return
        time_compare = last_session_feedback_data.index[0] < utc.localize(quarter_start_date)
        
        if time_compare:
            query = "select * from session_feedback_data order by time desc limit 1"
        else:
            query = "select * from session_feedback_data where " \
                "time >= '{quarter_start_date}' and time <= '{today}'"
                
        
        formatted_query = query.format(**quarter_info)
        session_feedback_df = influx.fetch_all(formatted_query)
        session_feedback_df['label'] = session_feedback_df.apply(
            lambda row: get_label(int(row['user_rating'] or '0')),
            axis=1
        )
        
        session_feedback_count_df = session_feedback_df.groupby(["user_id", "pps_mode"], as_index=False).apply(
            lambda df: pd.Series({
                'Count_P':  len(list(filter(lambda label: label == 'P', df['label'].tolist()))),
                'Count_D':  len(list(filter(lambda label: label == 'D', df['label'].tolist()))),
                'Count_N':  len(list(filter(lambda label: label == 'N', df['label'].tolist()))),
                'Count_Nothing':  len(list(filter(lambda label: label == 'Nothing', df['label'].tolist()))),
            })
        )

        session_feedback_count_df['Total'] = session_feedback_count_df.apply(
            lambda row: (row['Count_P'] or 0) + (row['Count_D'] or 0) + (row['Count_N'] or 0),
            axis=1
        )
        session_feedback_count_df['percent_P'] = session_feedback_count_df.apply(
            lambda row: (row['Count_P'] or 0)/(row['Total'] or 1),
            axis=1
        )
        session_feedback_count_df['percent_D'] = session_feedback_count_df.apply(
            lambda row: (row['Count_D'] or 0)/(row['Total'] or 1),
            axis=1
        )
        session_feedback_count_df['percent_N'] = session_feedback_count_df.apply(
            lambda row: (row['Count_N'] or 0)/(row['Total'] or 1),
            axis=1
        )
        session_feedback_count_df['label'] = session_feedback_count_df.apply(get_agg_label, axis=1)
        session_feedback_agg_df = session_feedback_count_df.groupby(
            ['pps_mode', 'label'], as_index=False
        ).apply(lambda df: pd.Series({'Count': len(df.index)}))

        transposed_session_feedback_agg_data = session_feedback_agg_df.pivot(index='pps_mode', columns='label')['Count']

        for col in  set(['Promoter', 'Detractor', 'Neutral', 'Nothing']) - set(transposed_session_feedback_agg_data.columns):
            transposed_session_feedback_agg_data = transposed_session_feedback_agg_data.assign(**{col:0})


        transposed_session_feedback_agg_data['NPS_ops_support'] = transposed_session_feedback_agg_data.apply(
            lambda row: round(
                (row.get('Promoter') - row.get('Detractor') ) / 
                ((row.get('Detractor') + row.get('Promoter') + row.get('Neutral')) or 1),
                2
            ),
            axis=1
        )

        transposed_session_feedback_agg_data['NPS_ops_support'] = transposed_session_feedback_agg_data.apply(
            lambda row: 0.5 if not row['NPS_ops_support'] else row['NPS_ops_support'], axis=1)
        print("transposed_session_feedback_agg_data", transposed_session_feedback_agg_data)

        if time_compare:
            transposed_session_feedback_agg_data['Detractor'] = 0
            transposed_session_feedback_agg_data['Promoter'] = 0
            transposed_session_feedback_agg_data['Neutral'] = 0
            transposed_session_feedback_agg_data['NPS_ops_support'] = 0.5
            row_count = 3
        else:
            row_count = 1

        session_feedback_final_df = pd.DataFrame()
        for i in range(1, row_count + 1):
            session_feedback_final_df = pd.concat(
                [
                    session_feedback_final_df,
                    transposed_session_feedback_agg_data.assign(RowCount=i)
                ],
            )

        print("session_feedback_final_df", session_feedback_final_df)

        if time_compare:
            session_feedback_final_df['pps_mode_2'] = session_feedback_final_df.apply(
                lambda row: 'pick' if row['RowCount'] == 1 else 'put' if row['RowCount'] == 2 else 'audit',
                axis=1
            )
        else:
            session_feedback_final_df['pps_mode_2'] = session_feedback_final_df.index

        session_feedback_final_df = session_feedback_final_df[
            ['pps_mode_2', 'NPS_ops_support']
        ].pivot(columns='pps_mode_2')['NPS_ops_support'].reset_index()[0:1]
                

        for col in ['audit', 'pick', 'put']:
            session_feedback_final_df[col] =  session_feedback_final_df.apply(
                lambda row: 0.5 if col not in session_feedback_final_df.columns or np.isnan(row[col]) else row[col],
                axis=1
            )

        session_feedback_final_df = session_feedback_final_df.rename(columns={
            'audit': 'nps_operator_audit',
            'pick': 'nps_operator_pick',
            'put': 'nps_operator_put'
        })

        session_feedback_final_df = session_feedback_final_df.drop(columns=['pps_mode'])

        session_feedback_final_df.at[0, 'time'] = pd.DatetimeIndex(
            [quarter_info.get('yesterday')]
        )[0]

        session_feedback_final_df = session_feedback_final_df.set_index('time')
        session_feedback_final_df = session_feedback_final_df.apply(pd.to_numeric)
        
        session_feedback_final_df['site'] = [self.site.get('name')]
        print("session_feedback_final_df", session_feedback_final_df)
        print("session_feedback_final_df", json.loads(session_feedback_final_df.to_json(orient='records')))
        write_influx = self.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        success = write_influx.write_all(session_feedback_final_df, 'nps_operator_ayx', dag_name=os.path.basename(__file__), site_name=self.site.get('name'), tag_columns=["site"])
        return

MultiTenantDAG(
    dag,
    [
       'nps_ops_support_ayx',
       'nps_operator_ayx'
    ],
    [
    ],
    NpsOperatorAYX
).create()
