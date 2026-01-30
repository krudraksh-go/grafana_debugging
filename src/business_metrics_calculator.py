
from numpy import mean
import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowTaskTimeout
from datetime import timedelta, datetime, timezone
import pandas as pd
import pytz
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, SingleTenantDAGBase
import os
import numpy as np
from config import (
    rp_one_year
)

class MetricCalculator:

    def get_flow_df(self):
        neglect_type = "p1|p2|p3|combine"
        q = f"select * from pps_information where time > now()-365d and Type!~/{neglect_type}/"
        df = self.fetch_data(q,fetch_one=False, db='Alteryx', format="dataframe")
        df = df.groupby('Type')['pps_id'].apply(lambda x: ','.join(x.astype(str))).reset_index()
        df = df.rename(columns={'Type': 'flow'})
        return df

    def get_mean_working_time(self, flow, extra_where_clause):
        q = f"select mean(Actual_operator_working_time)/1000 as working_time from operator_working_time_summary_airflow where Actual_operator_working_time<550000 and time>'{self.start_date}' and time<='{self.end_date}' "   +  extra_where_clause
        total_working_time = self.fetch_data(q,db='Alteryx')
        return total_working_time['working_time'] if total_working_time else 0
    
    def get_ecom_retail_working_time(self,flow,extra_where_clause, type):
        q = f"select mean(value)/1000 as working_time from custom_ppstask_events where time>'{self.start_date}' and time<='{self.end_date}' and value<550000 and event='rack_started_to_depart_pps' and task_type='pick' and order_flow = '{type}' "  +  extra_where_clause
        total_working_time = self.fetch_data(q,db='Alteryx')
        return total_working_time['working_time'] if total_working_time else 0
    
    def get_working_time(self,flow, extra_where_clause):
        total_working_time = self.get_mean_working_time(flow, extra_where_clause)
        ecom_working_time = self.get_ecom_retail_working_time(flow,extra_where_clause, 'online')
        retail_working_time = self.get_ecom_retail_working_time(flow,extra_where_clause, 'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [total_working_time],
            'metric':['owt_per_face'],
            'ecom': [ecom_working_time],
            'retail': [retail_working_time]
        })    
        return df
    

    def get_r2r_time(self, flow, extra_where_clause):
        q = f"select mean(value)/1000 as r2r_time from  r2r_waittime_calculation WHERE time>'{self.start_date}' and time<='{self.end_date}' and task_type='pick' and event='rack_arrived_to_pps'  and value<550000" +  extra_where_clause
        r2r_time = self.fetch_data(q,db='Alteryx')
        return r2r_time['r2r_time'] if r2r_time else 0
    
    def get_ecom_retail_r2r_time(self,flow,extra_where_clause, type):
        q = f"select mean(value)/1000 as r2r_time from custom_ppstask_events where time>'{self.start_date}' and time<='{self.end_date}' and value<550000 and event='rack_arrived_to_pps' and task_type='pick' and order_flow = '{type}' " +  extra_where_clause
        r2r_time = self.fetch_data(q,db='Alteryx')
        return r2r_time['r2r_time'] if r2r_time else 0

    def get_r2r_waiting_time(self, flow, extra_where_clause):
        r2r_time = self.get_r2r_time(flow, extra_where_clause)
        ecom_r2r_time = self.get_ecom_retail_r2r_time(flow,extra_where_clause,  'online')
        retail_r2r_time = self.get_ecom_retail_r2r_time(flow,extra_where_clause, 'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [r2r_time],
            'metric':['r2r'],
            'ecom': [ecom_r2r_time],
            'retail': [retail_r2r_time]
        })
        return df
    
    def get_ppf(self, flow, extra_where_clause):
        q = f"select mean(total_picks_value) as ppf from picks_per_rack_face where time>'{self.start_date}' and time<='{self.end_date}' " + extra_where_clause
        picks = self.fetch_data(q,db='GreyOrange')
        return picks['ppf'] if picks else 0
    
    def get_ecom_retail_ppf(self, flow, extra_where_clause,  type):
        q = f"select mean(total_picks_value) as ppf from picks_per_rack_face_ecom_retail where time>'{self.start_date}' and time<='{self.end_date}' and order_flow = '{type}' "  + extra_where_clause
        r2r_time = self.fetch_data(q, db='GreyOrange')
        return r2r_time['ppf'] if r2r_time else 0
    
    def picks_per_face(self, flow, extra_where_clause):
        ppf = self.get_ppf(flow, extra_where_clause)
        ecom_ppf = self.get_ecom_retail_ppf(flow, extra_where_clause,  'online')
        retail_ppf = self.get_ecom_retail_ppf(flow, extra_where_clause,  'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [ppf],
            'metric':['ppf'],
            'ecom': [ecom_ppf],
            'retail': [retail_ppf]
        })
        return df
    
    def get_item_picked(self, flow, extra_where_clause):
        q = f"select sum(total_picks_value) as item_picked from picks_per_rack_face where time>'{self.start_date}' and time<='{self.end_date}' " + extra_where_clause
        picks = self.fetch_data(q,db='GreyOrange')
        return picks['item_picked'] if picks else 0
    
    def get_ecom_retail_item_picked(self, flow, extra_where_clause,  type):
        q = f"select sum(total_picks_value) as item_picked from picks_per_rack_face_ecom_retail where time>'{self.start_date}' and time<='{self.end_date}' and order_flow = '{type}' " + extra_where_clause
        picks = self.fetch_data(q, db='GreyOrange')
        return picks['item_picked'] if picks else 0
    
    def get_item_pick(self, flow, extra_where_clause):
        item_picked = self.get_item_picked(flow, extra_where_clause)
        ecom_item_picked = self.get_ecom_retail_item_picked(flow, extra_where_clause,  'online')
        retail_item_picked = self.get_ecom_retail_item_picked(flow, extra_where_clause,  'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [item_picked],
            'metric':['units_per_day'],
            'ecom': [ecom_item_picked],
            'retail': [retail_item_picked]
        })
        return df
    
    def fetch_uph(self, flow, extra_where_clause, r2r_time, ppf, item_picked):
        q = f"select sum(Actual_operator_working_time)/1000 as working_time from operator_working_time_summary_airflow where Actual_operator_working_time<550000 and time>'{self.start_date}' and time<='{self.end_date}' "   +  extra_where_clause
        total_owt = self.fetch_data(q,db='Alteryx')
        total_owt =  total_owt['working_time'] if total_owt else 0  
        uph = (3600*ppf)/(r2r_time + ppf * (total_owt/item_picked))   
        return uph
    
    def fetch_ecom_retail_uph(self, flow, extra_where_clause, type):
        q = f"select mean(UPH_60min_value) as uph from interval_throughput_ecom_retail where time>'{self.start_date}' and time<='{self.end_date}' and order_flow = '{type}' " +  extra_where_clause
        uph = self.fetch_data(q, db='GreyOrange')
        return uph['uph'] if uph else 0

    def get_uph(self, flow, extra_where_clause, r2r_time, ppf, item_picked):
        uph = self.fetch_uph(flow, extra_where_clause, r2r_time, ppf, item_picked)
        ecom_uph = self.fetch_ecom_retail_uph(flow,extra_where_clause,  'online')
        retail_uph = self.fetch_ecom_retail_uph(flow,extra_where_clause,  'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [uph],
            'metric':['uph'],
            'ecom': [ecom_uph],
            'retail': [retail_uph]
        })
        return df

    def get_waiting_time(self, flow, extra_where_clause):
        q = f"select mean(operator_waiting_time)/1000 as waiting_time from operator_working_time_summary_airflow where operator_waiting_time<550000 and time>'{self.start_date}' and time<='{self.end_date}' "   +  extra_where_clause
        waiting_time = self.fetch_data(q,db='Alteryx')
        waiting_time = waiting_time['waiting_time'] if waiting_time else 0
        ecom_r2r_time = self.get_ecom_retail_r2r_time(flow,extra_where_clause,  'online')
        retail_r2r_time = self.get_ecom_retail_r2r_time(flow,extra_where_clause, 'retail')        

        df = pd.DataFrame({
            'flow': [flow],
            'value': [waiting_time],
            'metric':['waiting_time'],
            'ecom': [ecom_r2r_time],
            'retail': [retail_r2r_time]
        })
        return df        
    

    def get_mean_put_working_time(self, flow, extra_where_clause):
        q = f"select mean(operator_working_time)/1000 as working_time from put_owt_summary where operator_working_time<550000 and time>'{self.start_date}' and time<='{self.end_date}' "   +  extra_where_clause
        if flow == 'RTP':
            q = f"select mean(value)/1000 as working_time from r2r_waittime_calculation where task_type='put' and event='rack_started_to_depart_pps'  and value<550000  and time>'{self.start_date}' and time<='{self.end_date}' " + extra_where_clause
        total_working_time = self.fetch_data(q,db='Alteryx')
        return total_working_time['working_time'] if total_working_time else 0
     

    def get_put_working_time(self, flow, extra_where_clause):
        mean_put_owt = self.get_mean_put_working_time(flow, extra_where_clause)
        # ecom_put_owt = self.get_ecom_retail_put_working_time(flow, 'online')
        # retail_put_owt = self.get_ecom_retail_put_working_time(flow, 'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [mean_put_owt],
            'metric':['put_working_time'],
            'ecom': [0],
            'retail': [0]
        })
        return df

    def get_put_r2r_time(self, flow, extra_where_clause):
        q = f"select mean(value)/1000 as r2r_time from  r2r_waittime_calculation WHERE time>'{self.start_date}' and time<='{self.end_date}' and task_type='put' and event='rack_arrived_to_pps'  and value<550000" +  extra_where_clause
        r2r_time = self.fetch_data(q,db='Alteryx')
        return r2r_time['r2r_time'] if r2r_time else 0     

    def get_ecom_retail_put_r2r_time(self,flow, extra_where_clause,  type):
        q = f"select sum(value)/1000 as r2r_time from custom_ppstask_events where time>'{self.start_date}' and time<='{self.end_date}' and value<550000 and event='rack_arrived_to_pps' and task_type='put' and order_flow = '{type}' "
        r2r_time = self.fetch_data(q,db='Alteryx')
        return r2r_time['r2r_time'] if r2r_time else 0

    def get_put_r2r_waiting_time(self, flow, extra_where_clause):
        mean_put_r2r_time = self.get_put_r2r_time(flow, extra_where_clause)
        ecom_put_r2r_time = self.get_ecom_retail_put_r2r_time(flow,extra_where_clause,  'online')
        retail_put_r2r_time = self.get_ecom_retail_put_r2r_time(flow,extra_where_clause,  'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [mean_put_r2r_time],
            'metric':['put_r2r_waiting_time'],
            'ecom': [ecom_put_r2r_time],
            'retail': [retail_put_r2r_time]
        })
        return df
    

    def fetch_put_uph(self, flow, extra_where_clause, mean_put_r2r_time, ppf, item_put):
        q = f"select sum(operator_working_time)/1000 as working_time from put_owt_summary where operator_working_time<550000 and time>'{self.start_date}' and time<='{self.end_date}' "   +  extra_where_clause
        if flow == 'RTP':
            q = f"select sum(value)/1000 as working_time from r2r_waittime_calculation where task_type='put' and event='rack_started_to_depart_pps'  and value<550000  and time>'{self.start_date}' and time<='{self.end_date}' " + extra_where_clause
        total_owt = self.fetch_data(q,db='Alteryx')
        total_owt =  total_owt['working_time'] if total_owt else 0  
        uph = (3600*ppf)/(mean_put_r2r_time + ppf * (total_owt/item_put))   
        return uph
    
    def get_put_uph(self, flow, extra_where_clause, mean_put_r2r_time, ppf, item_put):
        uph = self.fetch_put_uph(flow, extra_where_clause, mean_put_r2r_time, ppf, item_put)
        # ecom_uph = self.fetch_ecom_retail_uph(flow,extra_where_clause,  'online')
        # retail_uph = self.fetch_ecom_retail_uph(flow, extra_where_clause,  'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [uph],
            'metric':['put_uph'],
            'ecom': [0],
            'retail': [0]
        })
        return df
    
    def get_put_ppf(self, flow, extra_where_clause):
        q = f"select mean(total_put_value) as ppf from put_per_rack_face where time>'{self.start_date}' and time<='{self.end_date}' " + extra_where_clause
        picks = self.fetch_data(q,db='GreyOrange')
        return picks['ppf'] if picks else 0
    

    def puts_per_face(self, flow, extra_where_clause):
        ppf = self.get_put_ppf(flow, extra_where_clause)
        # ecom_ppf = self.get_ecom_retail_puts_per_face(flow, 'online')
        # retail_ppf = self.get_ecom_retail_puts_per_face(flow, 'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [ppf],
            'metric':['puts_per_face'],
            'ecom': [0],
            'retail': [0]
        })
        return df

    def get_total_item_put(self, flow, extra_where_clause):
        q = f"select sum(total_put_value) as item_put from put_per_rack_face where time>'{self.start_date}' and time<='{self.end_date}' " + extra_where_clause
        picks = self.fetch_data(q,db='GreyOrange')
        return picks['item_put'] if picks else 0
    
    def get_item_put(self, flow, extra_where_clause):
        item_put = self.get_total_item_put(flow, extra_where_clause)
        # ecom_item_put = self.get_ecom_retail_item_put(flow, 'online')
        # retail_item_put = self.get_ecom_retail_item_put(flow, 'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [item_put],
            'metric':['items_put'],
            'ecom': [0],
            'retail': [0]
        })
        return df

    def get_total_orders(self, flow, extra_where_clause):
        q = f"select count(bintags) as total_orders from order_events where time>'{self.start_date}' and time<='{self.end_date}' and event_name='order_created' " + extra_where_clause
        orders = self.fetch_data(q,db='GreyOrange')
        return orders['total_orders'] if orders else 0
    
    def get_ecom_retail_orders(self, flow, extra_where_clause):
        q = f"select * from order_events where time>'{self.start_date}' and time<='{self.end_date}' and event_name = 'order_created' " + extra_where_clause
        data = self.fetch_data(q,fetch_one=False,db='GreyOrange')
        df = pd.DataFrame(data)
        if df.empty:
            return 0, 0
        if 'order_flow_name' not in df.columns:
            df['order_flow_name'] = ''
        df = self.CommonFunction.flow_event(df, self.site.get('name'))
        group_df = df.groupby('order_flow').count()
        ecom_orders = group_df.loc['online', 'order_id'] if 'online' in group_df.index else 0
        retail_orders = group_df.loc['retail', 'order_id'] if 'retail' in group_df.index else 0
        return ecom_orders, retail_orders

    def get_orders_per_day(self, flow, extra_where_clause):
        total_orders = self.get_total_orders(flow, extra_where_clause)
        ecom_orders, retail_orders = self.get_ecom_retail_orders(flow, extra_where_clause)
        df = pd.DataFrame({
            'flow': [flow],
            'value': [total_orders],
            'metric':['orders_per_day'],
            'ecom': [ecom_orders],
            'retail': [retail_orders]
        })
        return df

    def get_total_completed_orders(self, flow, extra_where_clause):
        q = f"SELECT count(distinct order_id) as total_orders FROM item_picked where time>'{self.start_date}' and time<='{self.end_date}' " + extra_where_clause
        orders = self.fetch_data(q,db='GreyOrange')
        return orders['total_orders'] if orders else 0
    

    def get_units_per_order(self, flow, units_per_order, ecom_units_per_order,retial_units_per_order  ):
        df = pd.DataFrame({
            'flow': [flow],
            'value': [units_per_order],
            'metric':['units_per_order'],
            'ecom': [ecom_units_per_order],
            'retail': [retial_units_per_order]
        })
        return df

    def get_order_line_per_order(self, flow, ol_per_order, ecom_ol_per_order,retail_ol_per_order  ):
        df = pd.DataFrame({
            'flow': [flow],
            'value': [ol_per_order],
            'metric':['ol_per_Order'],
            'ecom': [ecom_ol_per_order],
            'retail': [retail_ol_per_order]
        })
        return df

    def get_unit_per_ol(self, flow, unit_per_ol, ecom_unit_per_ol, retail_unit_per_ol ):
        df = pd.DataFrame({
            'flow': [flow],
            'value': [unit_per_ol],
            'metric':['unit_per_ol'],
            'ecom': [ecom_unit_per_ol],
            'retail': [retail_unit_per_ol]
        })
        return df
    
    def get_single_item_orders( self, flow, single_item_orders, ecom_single, retail_single):
        df = pd.DataFrame({
            'flow': [flow],
            'value': [single_item_orders],
            'metric':['single_items_order'],
            'ecom': [ecom_single],
            'retail': [retail_single]
        })
        return df
    
    def get_multi_item_orders( self, flow, multi_item_orders, ecom_multi, retail_multi):
        df = pd.DataFrame({
            'flow': [flow],
            'value': [multi_item_orders],
            'metric':['multi_items_order'],
            'ecom': [ecom_multi],
            'retail': [retail_multi]
        })
        return df
    
    def get_ecom_retail_completed_orders(self, flow, df):
        if df.empty:
            return 0, 0
        if 'order_flow_name' not in df.columns:
            df['order_flow_name'] = ''
        df['order_flow_name'] = df['order_flow_name'].apply(lambda x:'' if pd.isna(x) else x)
        df = self.CommonFunction.flow_event(df, self.site.get('name'))
        group_df = df.groupby('order_flow').nunique()
        ecom_orders = group_df.loc['online', 'order_id'] if 'online' in group_df.index else 0
        retail_orders = group_df.loc['retail', 'order_id'] if 'retail' in group_df.index else 0
        return ecom_orders, retail_orders
    
    def get_single_multi_orders(self, flow, extra_where_clause, df):
        if df.empty:
            return 0, 0
        if 'num_of_orderlines' not in df:
            df['num_of_orderlines'] = 1        
        group_df = df.groupby(['order_id'], as_index=False).agg(
            items = ('num_of_orderlines','sum') )
        group_df['type'] = group_df['items'].apply(lambda x : 'single' if x<=1 else 'multi')
        df = group_df.groupby(['type'], as_index=False).agg(
            orders = ('items','sum') )
        order_counts = df.set_index('type')['orders'].to_dict()
        single = order_counts.get('single', 0)
        multi = order_counts.get('multi', 0)
        total = single+multi
        single = 100 * single / total
        multi = 100 * multi / total
        return single, multi

    def get_single_multi_ecom_retail_orders(self, flow, extra_where_clause, df):
        if df.empty:
            return 0, 0, 0, 0
        if 'order_flow_name' not in df.columns:
            df['order_flow_name'] = ''
        df = self.CommonFunction.flow_event(df, self.site.get('name'))
        if 'num_of_orderlines' not in df:
            df['num_of_orderlines'] = 1        
        group_df = df.groupby(['order_flow','order_id'], as_index=False).agg(
            items = ('num_of_orderlines','sum') )
        group_df['type'] = group_df['items'].apply(lambda x : 'single' if x<=1 else 'multi')
        df = group_df.groupby(['order_flow','type'], as_index=False).agg(
            orders = ('items','sum') )
        ecom_single = 0
        ecom_multi = 1
        ecom_total = ecom_single+ecom_multi
        ecom_single = 100 * ecom_single / ecom_total
        ecom_multi = 100 * ecom_multi / ecom_total

        retail_single = 0
        retail_multi = 1
        retail_total = retail_single+retail_multi
        retail_single = 100 * retail_single / retail_total
        retail_multi = 100 * retail_multi / retail_total
        return ecom_single, ecom_multi, retail_single, retail_multi
    

    def get_total_order_lines(self, flow, extra_where_clause):
        q = f"select count(distinct orderline_ids) as ol from item_picked where  time>'{self.start_date}' and time<='{self.end_date}' " + extra_where_clause
        orderlines  = self.fetch_data(q,db='GreyOrange')
        return orderlines['ol'] if orderlines else 0
    
    def get_ecom_retail_order_lines(self, flow, extra_where_clause, df):
        if df.empty:
            return 0, 0
        if 'order_flow_name' not in df.columns:
            df['order_flow_name'] = ''
        df = self.CommonFunction.flow_event(df, self.site.get('name'))
        group_df = df.groupby('order_flow').nunique()
        ecom_ol = group_df.loc['online', 'order_id'] if 'online' in group_df.index else 0
        retail_ol = group_df.loc['retail', 'order_id'] if 'retail' in group_df.index else 0
        return ecom_ol, retail_ol        

    

    def get_order_line_per_day(self, flow, ol, ecom_ol, retail_ol):
        df = pd.DataFrame({
            'flow': [flow],
            'value': [ol],
            'metric':['order_lines_per_day'],
            'ecom': [ecom_ol],
            'retail': [retail_ol]
        })
        return df

    def get_single_item_order_percentage(self, flow, extra_where_clause):
        total_single_item_order_percentage = self.get_total_single_item_order_percentage(flow, extra_where_clause)
        ecom_single_item_order_percentage = self.get_ecom_retail_single_item_order_percentage(flow, 'online')
        retail_single_item_order_percentage = self.get_ecom_retail_single_item_order_percentage(flow, 'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [total_single_item_order_percentage],
            'metric':['single_item_order_percentage'],
            'ecom': [ecom_single_item_order_percentage],
            'retail': [retail_single_item_order_percentage]
        })
        return df

    def get_multi_item_order_percentage(self, flow, extra_where_clause):
        total_multi_item_order_percentage = self.get_total_multi_item_order_percentage(flow, extra_where_clause)
        ecom_multi_item_order_percentage = self.get_ecom_retail_multi_item_order_percentage(flow, 'online')
        retail_multi_item_order_percentage = self.get_ecom_retail_multi_item_order_percentage(flow, 'retail')
        df = pd.DataFrame({
            'flow': [flow],
            'value': [total_multi_item_order_percentage],
            'metric':['multi_item_order_percentage'],
            'ecom': [ecom_multi_item_order_percentage],
            'retail': [retail_multi_item_order_percentage]
        })
        return df
    
    def get_item_picked_df(self, flow, extra_where_clause):
        q = f"select * from item_picked where  time>'{self.start_date}' and time<='{self.end_date}' " + extra_where_clause 
        df = self.fetch_data(q,fetch_one = False, db='GreyOrange', format="dataframe")
        return df


    
    def generate_metrics(self, flow, extra_where_clause, **kwargs):

        print("start metrics")

        # pick metrics
        df1 = self.get_working_time(flow, extra_where_clause)
        df2 = self.get_r2r_waiting_time(flow, extra_where_clause)
        df3 = self.picks_per_face(flow, extra_where_clause)
        df4 = self.get_item_pick(flow, extra_where_clause)

        r2r_time = df2['value'].sum()
        ppf = df3['value'].sum()
        item_picked = df4['value'].sum()
        df5 = self.get_uph(flow, extra_where_clause, r2r_time, ppf, item_picked)
        df6 = self.get_waiting_time(flow, extra_where_clause)

        print("pick metrics done")

        df_item_picked = self.get_item_picked_df(flow, extra_where_clause)

        # put metrics
        df7 = self.get_put_working_time(flow, extra_where_clause)
        df8 = self.get_put_r2r_waiting_time(flow, extra_where_clause)
        df9 = self.puts_per_face(flow, extra_where_clause)
        df10 = self.get_item_put(flow, extra_where_clause)

        r2r_time = df8['value'].sum()
        ppf = df9['value'].sum()
        item_put = df10['value'].sum()
        df11 = self.get_put_uph(flow, extra_where_clause,r2r_time, ppf ,item_put )

        print("put metrics done")


        # order metrics
        df12 = self.get_orders_per_day(flow, extra_where_clause)

        total_units = df4['value'].sum()
        ecom_units = df4['ecom'].sum()
        retail_units = df4['retail'].sum()
        total_completed_orders = self.get_total_completed_orders(flow, extra_where_clause)
        ecom_completed_orders, retail_completed_orders = self.get_ecom_retail_completed_orders(flow, df_item_picked)
        units_per_order = total_units / total_completed_orders if total_completed_orders>0 else 0
        ecom_units_per_order = ecom_units / ecom_completed_orders if ecom_completed_orders>0 else 0
        ecom_units_per_order = ecom_units_per_order if ecom_completed_orders>0 else 0
        retial_units_per_order = retail_units / retail_completed_orders if retail_completed_orders>0 else 0
        retial_units_per_order = retial_units_per_order if retail_completed_orders>0 else 0
        df13 = self.get_units_per_order(flow, units_per_order,ecom_units_per_order,retial_units_per_order)


        total_order_lines = self.get_total_order_lines(flow, extra_where_clause)
        ecom_ol, retail_ol = self.get_ecom_retail_order_lines(flow, extra_where_clause, df_item_picked)
        ol_per_order = total_order_lines/total_completed_orders if total_completed_orders>0 else 0
        ecom_ol_per_order = ecom_ol/ecom_completed_orders if ecom_completed_orders>0 else 0
        retail_ol_per_order = retail_ol/retail_completed_orders if retail_completed_orders>0 else 0
        ecom_ol_per_order = ecom_ol_per_order if ecom_completed_orders>0 else 0
        retail_ol_per_order = retail_ol_per_order if retail_completed_orders>0 else 0
        df14 = self.get_order_line_per_order(flow, ol_per_order, ecom_ol_per_order, retail_ol_per_order)

        unit_per_ol = total_units / total_order_lines if total_order_lines>0 else 0
        ecom_unit_per_ol = ecom_units / ecom_ol if ecom_ol >0 else 0
        retail_unit_per_ol = retail_units / retail_ol if retail_ol >0 else 0
        ecom_unit_per_ol = ecom_unit_per_ol if ecom_ol else 0
        retail_unit_per_ol = retail_unit_per_ol if retail_ol else 0
        df15 = self.get_unit_per_ol(flow, unit_per_ol, ecom_unit_per_ol, retail_unit_per_ol)

        single_item_orders, multi_item_orders = self.get_single_multi_orders( flow, extra_where_clause, df_item_picked)
        ecom_single, ecom_multi, retail_single, retail_multi = self.get_single_multi_ecom_retail_orders( flow, extra_where_clause, df_item_picked)
        ecom_single = ecom_single if ecom_completed_orders else 0
        ecom_multi = ecom_multi if ecom_completed_orders else 0
        retail_single = retail_single if retail_completed_orders else 0
        retail_multi = retail_multi if retail_completed_orders else 0

        df16 = self.get_single_item_orders( flow, single_item_orders, ecom_single, retail_single)
        df17 = self.get_multi_item_orders( flow, multi_item_orders, ecom_multi, retail_multi)

        print("order metrics done")

        result = pd.concat([df1, df2, df3, df4, df5,df6,df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17], ignore_index=True)
        result['value'] = result['value'].fillna(0).round(2)
        result.replace([np.inf, -np.inf], 0, inplace=True)
        return result

    def fetch_data(self, query, fetch_one=True, format="json", db = 'GreyOrange'):
        influx = self.CommonFunction.get_site_read_influx_client(site=self.site,database=db)
        print(query)
        if fetch_one:
            return influx.fetch_one(query, format=format)
        return influx.fetch_all(query, format=format)
        

    def add_flow_to_df(self, df, flow_df):
        if flow_df.shape[0] == 1:
            flow = flow_df.iloc[0]['flow']
            df['flow'] = flow
        else:
            df = pd.merge(df, flow_df, on='pps_id', how='left')
        return df

    def get_datetime_interval(self):
        q = f"select * from key_business_metrics where site='{self.site.get('name')}'  order by time desc limit 1"
        central_influx_client = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        start_datetime = central_influx_client.fetch_one(q)
        if not start_datetime.empty:
            start_datetime = pd.to_datetime(start_datetime.index[0], utc=True)
            start_datetime =start_datetime+timedelta(days=1)
        else:
            start_datetime = datetime.now(timezone.utc) - timedelta(days=10)
        start_time = self.site.get('starttime')
        hour, minute, second = map(int, start_time.split(':'))
        start_datetime = start_datetime.replace(hour=hour, minute=minute, second=0, microsecond=0)

        end_datetime = datetime.now(timezone.utc)
        timeseriesdata = self.CommonFunction.create_time_series_data2(start_datetime,end_datetime, '1d')
        return timeseriesdata

    
    def update_today_date_type(self, df):
        peak_date_df = self.CommonFunction.get_sheet_from_airflow_setup_excel('peak_dates')
        site = self.site.get('name')
        df['day_type'] = 'non_peak'
        peak_date_df = peak_date_df[peak_date_df['Name'] == site]
        peak_date_df['peak_start'] = pd.to_datetime(peak_date_df['peak_start']).dt.tz_localize('UTC')
        peak_date_df['peak_end'] = pd.to_datetime(peak_date_df['peak_end']).dt.tz_localize('UTC')
        today_date = pd.to_datetime(self.start_date)
        for index, row in peak_date_df.iterrows():
            if today_date >= row['peak_start'] and today_date <= row['peak_end']:
                df['day_type'] = 'peak'   
        return df     

    def get_breach_status(self, my_value, expected_value, metric):
        less_than_metrics = ['r2r','put_r2r']
        if metric in less_than_metrics:
            return 1 if my_value > expected_value else 0
        else:
            return 1 if my_value < expected_value else 0

    def get_peak_values(self):
        df = self.CommonFunction.get_sheet_from_airflow_setup_excel('expected_values')   
        site = self.site.get('name')
        df = df[df['Name'] == site]
        columns = df.columns.tolist()
        final_column = []
        for column in columns:
            if 'sol' in column:
                final_column.append(column)
        df = df[final_column]
        df = df.T.reset_index()
        df.columns = ['metric', 'value']
        df['value'] =  df['value'].astype(str)
        df['peak'] = df['value'].apply(lambda x: x.split('|')[0])
        df['non_peak'] = df['value'].apply(lambda x: x.split('|')[1] if (len(x.split('|')) > 1) else 0)
        df = df.replace('NA', '0')
        df['peak'] = df['peak'].astype(float)
        df['non_peak'] = df['non_peak'].astype(float)
        df['metric'] = df['metric'].apply(lambda x: x.replace('sol__', ''))
        df['metric'] = df['metric'].apply(lambda x: x.replace('sol_', ''))
        df['flow'] = df['metric'].apply(lambda x: x.split('__')[1].upper() if '__' in x else 'RTP')
        df['type'] = df['metric'].apply(lambda x: x.split('__')[2].upper() if len(x.split('__')) > 2 else 'default')
        df['metric'] = df['metric'].apply(lambda x: x.split('__')[0])
        if 'value' in df.columns:
            del df['value']
        df = df.fillna(0)
        return df        
    

    def final_call(self, **kwargs):
        self.CommonFunction = CommonFunction()
        daterange = self.get_datetime_interval()
        print(daterange)
        for i in daterange.index:
            self.start_date = daterange['interval_start'][i].strftime('%Y-%m-%dT%H:%M:%SZ')
            self.end_date = daterange['interval_end'][i].strftime('%Y-%m-%dT%H:%M:%SZ')
            self.final_call1( **kwargs)     


    def final_call1(self, **kwargs):
        flow = self.site.get('flow')
        extra_where_clause = ""

        if flow in ['RTP', 'TTP']:
            result = self.generate_metrics(flow, extra_where_clause)

        else:
            flow_df = self.get_flow_df()
            if flow_df.empty:
                print(f"No Flow Found For Site: {self.site.get('name')} in table pps_information")
                return None
            for idx, row in flow_df.iterrows():
                flow = row['flow'].upper()
                pps = row['pps_id']
                print(f"flow: {flow}, pps: {pps}")
                extra_where_clause = f" and pps_id=~/{'|'.join(pps.split(','))}/"
                df = self.generate_metrics(flow, extra_where_clause)
                if idx == 0:
                    result = df
                else:
                    result = pd.concat([result, df], ignore_index=True)
        
        result = result.rename(columns={'value': 'default'})
        result = result.melt(id_vars=['flow', 'metric'], value_vars=['ecom', 'retail', 'default'],
                            var_name='type', value_name='value')
    
        peak_non_peak_df = self.get_peak_values()
        result = pd.merge(result, peak_non_peak_df, on=['metric', 'flow', 'type'], how='left')
        result = result.fillna(0)    
        result = self.update_today_date_type(result)
        day_type = result['day_type'].iloc[0]
        result['breach'] = 0
        result['breach'] = result.apply(lambda x: self.get_breach_status(x['value'], x[day_type], x['metric']), axis=1)

        site = self.site.get('name')
        result['site'] = site
        result['key'] = pd.to_datetime(self.start_date).strftime('%Y-%m-%d') + '_' + site
        result['key'] = result['key'] + '_' + result['flow'] + '_' + result['type'] + '_' + result['metric']
        result['time'] = pd.to_datetime(self.start_date)
        result['value'] = result['value'].round(2)
        result = result.set_index('time')
        write_influx = self.CommonFunction.get_central_influx_client(self.site.get('alteryx_out_db_name'), self.site.get('central_influx'))
        isvalid= write_influx.is_influx_reachable(dag_name=os.path.basename(__file__), site_name=self.site.get('name'))
        if isvalid:
            success = write_influx.write_all(result, 'key_business_metrics', dag_name=os.path.basename(__file__), site_name=self.site.get('name'), tag_columns=['site','metric','flow','type'])
            print("Business metrics calculation inserted")
        return None
