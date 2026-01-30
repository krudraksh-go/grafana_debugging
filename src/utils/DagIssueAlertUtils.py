from datetime import datetime
import redis
import json
import pandas as pd
import os
from collections import Counter
from influxdb import DataFrameClient
from config import (
    CENTRAL_INFLUX_USERNAME,
    CENTRAL_INFLUX_PASSWORD
)

def push_failure_to_central_influx(data):
    for key,value in data.items():
        data[key] = [value]
    df = pd.DataFrame(data)
    if not df.empty:
        try:
            df.rename(columns={'UTC Timestamp':'time','Error Type':'Error_Type','Error Message':'Error_Message'},inplace=True)
            df['time'] = pd.to_datetime(df['time'])
            df = df.set_index('time')
            write_influx = DataFrameClient(host='alteryx-influx.greymatter.greyorange.com', port=8086,
                username=CENTRAL_INFLUX_USERNAME,
                password=CENTRAL_INFLUX_PASSWORD,
                ssl=True,
                database="telegraf",
                retries=2,
            )
            success = write_influx.write_points(df, 'dag_issue_alert',database='telegraf', tag_columns=["Site","Error_Type"])
            print("Success! Data Pushed to Central Influx Server!!")
        except Exception as ex:
            error = str(ex)
            print(f"unable to push error to central influx error: {error}")

def push_failure_to_central_influx_mails(data):
    for key,value in data.items():
        data[key] = [value]
    df = pd.DataFrame(data)
    host_name = os.environ.get('GM_WORKER_NAME', '10.100.3.14')
    df['host_name']=host_name
    if not df.empty:
        try:
#            df.rename(columns={'UTC Timestamp':'time','Error Type':'Error_Type','Error Message':'Error_Message'},inplace=True)
            df['time'] = pd.to_datetime(df['time'])
            df = df.set_index('time')
            write_influx = DataFrameClient(host='alteryx-influx.greymatter.greyorange.com', port=8086,
                username=CENTRAL_INFLUX_USERNAME,
                password=CENTRAL_INFLUX_PASSWORD,
                ssl=True,
                database="telegraf",
                retries=2,
            )
            success = write_influx.write_points(df, 'alert_mails',database='telegraf')
            print("Success! Data Pushed to Central Influx Server!!")
        except Exception as ex:
            error = str(ex)
            print(f"unable to push error to central influx error: {error}")


def log_failures(Type, Origin, SiteName, Destination, Message):
    r = redis.StrictRedis(host='redis', port=6379, db=0)
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    data_json = {
        'Dag': Origin,
        'Site': SiteName,
        'Error Type': Type,
        'Datasource': Destination,
        'Error Message': Message,
        'UTC Timestamp': formatted_datetime
    }
    json_str = json.dumps(data_json)
    r.rpush(Type, json_str)
    print("data pushed to redis!!")
    # push data to central influx server
    if data_json['Error Type'] in ['db_connection_failure','influxdb_write_issue']:
        push_failure_to_central_influx(data_json)


def aggregate_error_counts_by_site(error_list):
    site_counts = {}

    for error in error_list:
        site = error['Site']
        error_type = error['Error Type']

        if site not in site_counts:
            site_counts[site] = {
                'Site': site,
                'db_connection_failure': 0,
                'influxdb_write_issue': 0,
                'dag_failure': 0,
                'total_failures': 0,
            }

        site_counts[site][error_type] += 1

    for site_data in site_counts.values():
        site_data['total_failures'] = (
            site_data['db_connection_failure'] +
            site_data['influxdb_write_issue'] +
            site_data['dag_failure']
        )

    result_list = list(site_counts.values())
    return result_list


def find_most_common_dag(data_list):
    origin_values = [item['Dag'] for item in data_list]
    origin_counter = Counter(origin_values)
    # Find the most common 'Dag' value and its count
    most_common_origin_count = origin_counter.most_common(1)
    
    if most_common_origin_count:
        most_common_origin, count = most_common_origin_count[0]
        return most_common_origin, count
    else:
        return None, 0  # Return None and 0 count if the list is empty

def create_alert_html_tables(json_list):
    consolidate_json_list = aggregate_error_counts_by_site(json_list)
    most_common_dag, most_common_dag_count = find_most_common_dag(json_list)
    df1 = pd.DataFrame(consolidate_json_list)
    df2 = pd.DataFrame(json_list)

    # Convert DataFrames to HTML tables
    html_table1 = df1.to_html(justify='left')
    html_table2 = df2.to_html(justify='left')

    html_document = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Airflow Dag Issues</title>
    </head>
    <body>
        <p>Most failed dag: {most_common_dag}</p>
        <p>Most failed dag count: {most_common_dag_count}</p>
        <h2>Summary</h2>
        {html_table1}
        <h2>Details</h2>
        {html_table2}
    </body>
    </html>
    """

    return html_document


def log_dag_failure(context):
    print(context['exception'].__class__.__name__)
    try:
        kwargs = context['task_instance'].task.python_callable.keywords
        SiteName = kwargs['tenant_info']['tenant_info']['Name']
    except Exception as ex:
        print("exception while trying to fetch SiteName...")
        log_failures('dag_failure', context['task_instance'].dag_id, 'NA', 'NA', str(context['exception']))
    else:
        # Define the list of possible errors already logged,
        # so don't log here in such cases.
        known_exception_list = [
            'DbConnectionError',
            'DbWriteError'
        ]
        exception_name = str(context['exception'].__class__.__name__)
        if exception_name not in known_exception_list:
            print("not in any known pattern, logging as dag failure")
            log_failures('dag_failure', context['task_instance'].dag_id, SiteName, 'NA', str(context['exception']))


## Custom exception classes for db connection and db write errors 
class DbConnectionError(Exception):
    """Custom exception for database connection errors."""

    def __init__(self, message="Database connection error occurred."):
        self.message = message
        super().__init__(self.message)


class DbWriteError(Exception):
    """Custom exception for database write errors."""

    def __init__(self, message="Database write error occurred."):
        self.message = message
        super().__init__(self.message)
