import pandas as pd
import requests
import psycopg2
import operator
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction, postgres_connection
import os
class InputDataHandler:
    def __init__(self, sheet_name):
        self.utilfunction = CommonFunction()
        df = self.utilfunction.get_sheet_from_airflow_setup_excel(sheet_name)
        self.df = df.reset_index(drop=True)

    def filter_input_data(self, site_name):
        df = self.df
        specific_site_df = df[df['Site Name'] == site_name]
        specific_site_insights = specific_site_df['Insight'].unique()
        filtered_df = df[((df['Site Name'] == site_name) | (
                    (df['Site Name'] == 'All') & (~df['Insight'].isin(specific_site_insights))))]
        self.df = filtered_df.reset_index(drop=True)
        return self.df

    def filter_expected_data(self, site_name):
        filtered_row = self.df[self.df['Name'] == site_name]
        if not filtered_row.empty:
            filtered_row=filtered_row.reset_index()
        return filtered_row


class InsightHandler:
    def __init__(self, dict,start_date,end_date,tenant_info):
        self.tenant_info= tenant_info
        self.parameter = dict['Parameter']
        self.insight = dict['Insight']
        self.query1 = dict['Query1']
        self.query2 = dict['Query2']
        self.group_by_pps = dict['GroupByPPS']
        self.aggregator = dict['Aggregator']
        self.operator1 = dict['Operator1']
        self.operator2 = dict['Operator2']
        self.expected_values = dict['ExpectedValues']
        self.expected_values_from_query = dict['ExpectedValuesFromQuery']
        self.result_comparator = dict['ResultComparator']
        self.query1_datasource = dict['Query1_DataSource']
        self.query1_database = dict['Query1_Database']
        self.query2_datasource = dict['Query2_DataSource']
        self.query2_database = dict['Query2_Database']
        self.expected_values_from_query__datasource = dict['ExpectedValuesFromQuery_DataSource']
        self.expected_values_from_query_database = dict['ExpectedValuesFromQuery_Database']
        self.suggestion_on_breach = dict['SuggestionOnBreach']
        self.start_date= start_date
        self.end_date=end_date

        # if self.query1_datasource == 'Influx':
        #     self.start_date = "2024-03-25T19:00:23.015761496Z"
        #     self.end_date = "2024-03-27T19:00:23.015761496Z"
        # # Postgres queries have fixed and limited data
        # if self.query1_datasource == 'Postgres':
        #     self.start_date = "2020-10-27T10:35:21.012346Z"
        #     self.end_date = "2020-11-03T03:09:45.81376Z"

        # filling start_date and end_date in the queries
        self.query1 = eval(f'f"""{self.query1}"""')
        if not pd.isna(self.query2):
            self.query2 = eval(f'f"""{self.query2}"""')
        if not pd.isna(self.expected_values_from_query):
            self.expected_values_from_query = eval(f'f"""{self.expected_values_from_query}"""')

        self.ops = {
            '+' : operator.add,
            '-' : operator.sub,
            '*' : operator.mul,
            '/' : operator.truediv,
            '<' : operator.lt,
            '<=' : operator.le,
            '>' : operator.gt,
            '>=' : operator.ge,
            '=' : operator.eq,
            'Equal': operator.eq
        }

    def handle_insight(self):
        # fetch the output of Query1
        query1_handler = QueryHandler(tenant_info= self.tenant_info, query=self.query1, database=self.query1_database, datasource=self.query1_datasource, group_by_pps=self.group_by_pps, aggregator=self.aggregator)
        query1_output = query1_handler.query_handler()
        if query1_output.empty:
            return query1_output

        # conditionally perdorm the following:
        # 1. fetch the output of Query2
        # 2. perform arithmetic Operation1
        if not pd.isna(self.query2):
            query2_handler = QueryHandler(tenant_info= self.tenant_info, query=self.query2, datasource=self.query2_datasource, database=self.query2_database, group_by_pps=self.group_by_pps, aggregator=self.aggregator)
            query2_output = query2_handler.query_handler()
            if query2_output.empty:
                return query1_output

            post_arithmetic_result = self.arithmetic_helper(query1_output, query2_output)
        else:
            post_arithmetic_result = query1_output

        # conditionally perform FinalCalculation using Operator2
        final_result = self.final_calculation_helper(post_arithmetic_result)

        # add a new column 'breach_status'
        # data_field contains the actual_values of Insight
        # compare with ExpectedValues using ResultComparator
        if 'data_field' in final_result.columns:
            final_result['breach_status'] = final_result['data_field'].apply(lambda x: self.comparator(x))
        else:
            print("Column 'data_field' does not exist in the DataFrame.")

        return final_result

    def arithmetic_helper(self, df1, df2):
        group_by_pps = self.group_by_pps
        if group_by_pps == "No":
            post_arithmetic_result = self.ops[self.operator1](df1, df2)
            return post_arithmetic_result
        else:
            # inner join on dataframes from both queries
            # to keep only those 'pps_id's which have matching values in both DataFrames
            merged_df = pd.merge(df1, df2, on = 'pps_id', suffixes = ('_df1', '_df2'))
            merged_df['data_field'] = self.ops[self.operator1](merged_df['data_field_df1'], merged_df['data_field_df2'])
            merged_df = merged_df.drop(columns=['data_field_df1', 'data_field_df2'])
            return merged_df

    def fetch_expected_values(self):
        query_handler = QueryHandler(tenant_info= self.tenant_info,query=self.expected_values_from_query, database=self.expected_values_from_query_database, datasource=self.expected_values_from_query__datasource)
        query_output = query_handler.query_handler()
        # print(f"EXPECTED QUERY OUTPUT: {query_output}")
        if query_output.empty:
            return 0
        else:
            return query_output['data_field'][0]

    def comparator(self, actual_value):
        comp = self.result_comparator
        if pd.isna(self.expected_values):
            self.expected_values = self.fetch_expected_values()
        return not self.ops[comp](actual_value, self.expected_values)

    def final_calculation_helper(self, result):
        final_calc = self.operator2
        if not pd.isna(final_calc):
            result['data_field'] = eval("result['data_field']" + self.operator2)
            return result
        return result


class QueryHandler:
    def __init__(self, tenant_info, query, database, datasource, group_by_pps='No', aggregator='mean'):
        self.query = query
        self.database = database
        self.datasource = datasource
        self.group_by_pps = group_by_pps
        self.aggregator = aggregator
        self.tenant_info = tenant_info

    def query_handler(self):
        # call the query helper function depending on datasource
        if self.datasource == "Influx":
            df = self.query_influxdb()
        if self.datasource == "Postgres":
            df = self.query_postgres()

        # the field/column containing *data is named 'data_field'
        # *data is the raw_data used to compute actual_value of insight
        if df.empty:
            return df

        if len(df.columns) == 2:
            if df.columns[0] == 'pps_id':
                df = df.rename(columns={df.columns[1]: 'data_field'})
            elif df.columns[1] == 'pps_id':
                df = df.rename(columns={df.columns[0]: 'data_field'})
            else:
                print("error: incorrect data fields")
                return pd.DataFrame()  # empty dataframe
        else:
            df = df.rename(columns={df.columns[0]: 'data_field'})

        # make sure data_field is float
        df['data_field'] = df['data_field'].astype(float)

        # (conditionally) group the data by pps_id
        if self.group_by_pps == "No":
            if pd.isna(self.aggregator):
                result = df
            else:
                result = df.agg([self.aggregator])
        else:
            result = df.groupby('pps_id').agg({'data_field': self.aggregator})

        # returns df of either type:
        # 2 columns: pps_id and data_field
        # 1 column: data_field
        return result

    def query_influxdb(self):
        # url = 'http://30.224.46.14:8086/query'
        # params = {
        #     'db': self.database,
        #     'q': self.query
        # }
        self.read_client = InfluxData(host=self.tenant_info["influx_ip"], port=self.tenant_info["influx_port"],
                                      db=self.database)
        isvalid = self.read_client.is_influx_reachable(host=self.tenant_info["influx_ip"],
                                                  port=self.tenant_info["influx_port"],
                                                  dag_name=os.path.basename(__file__),
                                                  site_name=self.tenant_info['Name'])
        if not isvalid:
            raise ValueError('InfluxDB not connected')

        # response = requests.get(url, params=params)
        # result = response.json()
        # # print(f"result = {result}")
        # series_data = result['results'][0]['series'][0]
        # df = pd.DataFrame(series_data['values'], columns=series_data['columns'])
        df = pd.DataFrame(self.read_client.query(self.query).get_points())
        #print(self.query)
        #print(df)
        if not df.empty:
            df = df.drop(columns=['time'])

            # 'pps_id' fetched from InfluxDB are inside quotation
            # but those from Postgres don't have it
            # removing quotations
            if 'pps_id' in df.columns:
                df['pps_id'] = df['pps_id'].astype(str)
                df['pps_id'] = df['pps_id'].str.replace("\"", "")

        return df

    def query_postgres(self):
        # conn = psycopg2.connect(
        #     dbname=self.database,
        #     user="postgres",
        #     password="password",
        #     host='172.17.0.2',
        #     port='5432'
        # )
        if self.database=='tower':
            self.postgres_conn = postgres_connection(database='tower', user=self.tenant_info['Tower_user'], \
                                                     sslrootcert=self.tenant_info['sslrootcert'],
                                                     sslcert=self.tenant_info['sslcert'], \
                                                     sslkey=self.tenant_info['sslkey'],
                                                     host=self.tenant_info['Postgres_tower'], \
                                                     port=self.tenant_info['Postgres_tower_port'],
                                                     password=self.tenant_info['Tower_password'],
                                                     dag_name=os.path.basename(__file__),
                                                     site_name=self.tenant_info['Name'])
        elif self.database=='platform_srms':
            self.postgres_conn = postgres_connection(database='platform_srms', user=self.tenant_info['Postgres_pf_user'], \
                                                     sslrootcert=self.tenant_info['sslrootcert'],
                                                     sslcert=self.tenant_info['sslcert'], \
                                                     sslkey=self.tenant_info['sslkey'],
                                                     host=self.tenant_info['Postgres_pf'], \
                                                     port=self.tenant_info['Postgres_pf_port'],
                                                     password=self.tenant_info['Postgres_pf_password'],
                                                     dag_name=os.path.basename(__file__),
                                                     site_name=self.tenant_info['Name'])
        else:
            self.postgres_conn = postgres_connection(database='butler_dev', user=self.tenant_info['Postgres_butler_user'], \
                                                     sslrootcert=self.tenant_info['sslrootcert'],
                                                     sslcert=self.tenant_info['sslcert'], \
                                                     sslkey=self.tenant_info['sslkey'],
                                                     host=self.tenant_info['Postgres_ButlerDev'], \
                                                     port=self.tenant_info['Postgres_butler_port'],
                                                     password=self.tenant_info['Postgres_butler_password'],
                                                     dag_name=os.path.basename(__file__),
                                                     site_name=self.tenant_info['Name'])

        # cursor = conn.cursor()
        # cursor.execute(self.query)
        # result = cursor.fetchall()
        #
        # column_names = [desc[0] for desc in cursor.description]
        #
        # cursor.close()
        # conn.close()
        result = self.postgres_conn.fetch_postgres_data_in_chunk(self.query)

        df = pd.DataFrame(result)

        return df

class OutputHandler:
    def __init__(self):
        self.final_df= pd.DataFrame()
    #     # df = pd.read_excel(output_file_path, output_sheet_name)
    #     # self.output_file_path = output_file_path
    #     # self.output_sheet_name = output_sheet_name
    #     # self.df = df.reset_index(drop=True)
    #     self.final_df= self.final_df.reset_index(drop=True)

    def append_result(self, output_df, insight_handler):
        for pps_id, info in output_df.iterrows():
            # iterating over output_df / insight_output
            data_field_value = info['data_field']
            breach_status_value = info['breach_status']
            if pd.isna(insight_handler.expected_values):
                insight_handler.expected_values = insight_handler.fetch_expected_values()
            row_data = {
            #'time': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            'actual_value': data_field_value,
            #'bot_in_error': 0, #check
            'expected_value': insight_handler.expected_values,
            'insights': insight_handler.insight,
            'parameter': insight_handler.parameter,
            'per_pps': insight_handler.group_by_pps
            }

            if insight_handler.group_by_pps == 'Yes':
                row_data['pps_id'] = pps_id
            else:
                row_data['pps_id'] = '0'

            if breach_status_value == True:
                row_data['result'] = "Breached"
            else:
                row_data['result'] = 'Non-Breached'

            self.final_df = self.final_df.append(row_data, ignore_index=True)
            #print(self.final_df)
            #self.final_df.to_excel(excel_writer = self.output_file_path, sheet_name = self.output_sheet_name, index = False)

class DynamicInsights:
    def __init__(self, start_date, end_date,tenant_info):
        self.tenant_info = tenant_info
        self.start_date = start_date
        self.end_date = end_date
        self.site_name = self.tenant_info["Name"]
        self.input_data_handler = InputDataHandler(sheet_name='insight_suggestion')
        self.filtered_input_data = self.input_data_handler.filter_input_data(self.site_name)
        self.expected_data_handler = InputDataHandler(sheet_name='expected_values')
        self.expected_data = self.expected_data_handler.filter_expected_data(self.site_name)
    def main(self):
        # expected_data_handler = self.utilfunction.get_sheet_from_airflow_setup_excel('expected_values')
        # input_data_handler = self.utilfunction.get_sheet_from_airflow_setup_excel('insight_suggestion')
        # source_path = "/home/aryan.g/Downloads/sheet_influx.xlsx"
        # sheet_name = "Dynamic"
        # site_name = self.tenant_info["Name"]
        #
        # input_data_handler = InputDataHandler('expected_values')
        # filtered_input_data = input_data_handler.filter_input_data(site_name)
        #pd.set_option('display.max_columns', None)
        #pd.set_option('display.max_rows', None)
        #print("filtered_input_data: \n", self.filtered_input_data)

        # expected_data_path = "/home/aryan.g/Downloads/sheet_influx.xlsx"
        # expected_data_sheet_name = "Expected Values"
        # expected_data_handler = InputDataHandler('insight_suggestion')
        # expected_data = expected_data_handler.filter_expected_data(site_name)

        #print(f"expected_data: {self.expected_data}")
        output_handler = OutputHandler()
        for i in range(len(self.filtered_input_data)):
            # iterating over the excel sheet entries (filtered)
            #print("row_number: ", i)
            row = self.filtered_input_data.loc[i]
            row_dict = row.to_dict()
            # print(f"ORIGINAL row_dict: {row_dict}")
            if (pd.isna(row_dict['ExpectedValuesFromQuery_DataSource']) | pd.isna(
                    row_dict['ExpectedValuesFromQuery']) | pd.isna(row_dict['ExpectedValuesFromQuery_Database'])):
                # print("EXPECTED VALUE FROM SHEET")
                insight_exp_field = row_dict['Expected_field']
                if insight_exp_field in self.expected_data.columns:
                    expected_value = self.expected_data[insight_exp_field][0]
                    # print(f"expectedvalue for {insight} is {expected_value}")
                    row_dict['ExpectedValues'] = expected_value
                if pd.isna(row_dict['ExpectedValues']):
                    print("NO EXPECTED VALUE FOUND")
                    continue
            # print(f"row_dict: {row_dict}")
            insight_handler = InsightHandler(row_dict,self.start_date,self.end_date,self.tenant_info)
            insight_output = insight_handler.handle_insight()
         #   print("insight_output: \n", insight_output)
            if 'breach_status' in insight_output.columns and 'data_field' in insight_output.columns and not insight_output.empty:
                output_handler.append_result(output_df=insight_output, insight_handler=insight_handler)
            # if not output_handler.empty:
            #     print(output_handler.final_df)
        output_handler.final_df.to_csv("temp.csv")
        return output_handler.final_df


#if __name__ == "__main__":
    # main()
