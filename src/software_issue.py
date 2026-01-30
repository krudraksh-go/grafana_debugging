import airflow
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime, timezone
import pandas as pd
from utils.CommonFunction import InfluxData, Write_InfluxData, CommonFunction,postgres_connection
from pandasql import sqldf
import pytz
import time
import numpy as np
import os
import psycopg2
import warnings
from jira.client import JIRA
from influxdb import InfluxDBClient, DataFrameClient
from jira.exceptions import JIRAError
from jira import JIRA
warnings.filterwarnings("ignore")


class SoftwareIssues():

    def final_call(self, tenant_info, **kwargs):
        jira_server = 'https://work.greyorange.com/jira/'
        jira_user = 'bot-analytics'
        jira_password = 'bot-analytics@123'
        jira_server = {'server': jira_server}
        jira = JIRA(options=jira_server, basic_auth=(jira_user, jira_password))
        
        self.tenant_info = tenant_info['tenant_info']
        end = datetime.today() - timedelta(days=100)
        end_date = end.strftime('%Y-%m-%d')

        print("Processing the results...This may take sometime please wait.")
        sev1 = jira.search_issues(jql_str="project = SS AND issuetype in ('Incident  (Butler)') AND cf[11017]=\"Breakdown (CSS-268)\"AND 'Start Date and Time'>='%s'" %end_date, maxResults=90000)
        sev2=  jira.search_issues(jql_str="project = SS AND issuetype in ('Incident  (Butler)') AND cf[11017]=\"Partial Breakdown (CSS-271)\"AND 'Start Date and Time'>='%s'" %end_date, maxResults=90000)
        df=0
        issue=0
        sev1_data=[]
        sev2_data=[]

        for issue in sev1:
            starttime=(issue.fields.customfield_11003)
            endtime=(issue.fields.customfield_11010)
            systemid=issue.fields.customfield_11005
            affected=issue.fields.customfield_11404
            SE=issue.fields.customfield_11018
            Category=issue.fields.customfield_12600
            Requester_group=issue.fields.customfield_11016
            Customer_name=issue.fields.customfield_10800
            SLA=issue.fields.customfield_11017
            Reporting_medium=issue.fields.customfield_11015
            Variant=issue.fields.customfield_11405
            labels=issue.fields.labels
            sev1_data.append([issue.key,issue.fields.summary,starttime,endtime,systemid,affected,SE,Category,Requester_group,Customer_name,SLA,Reporting_medium,Variant,labels,'sev1'])

        df1=pd.DataFrame(sev1_data)
        df1.columns=['Id','Summary','Starttime','Endtime','Systemid','Affected','SE','Category','Requester_group','Customer_name','SLA','Reporting_medium','Variant','labels','severity']

        for issue in sev2:
            starttime=(issue.fields.customfield_11003)
            endtime=(issue.fields.customfield_11010)
            systemid=issue.fields.customfield_11005
            affected = issue.fields.customfield_11404
            SE=issue.fields.customfield_11018
            Category=issue.fields.customfield_12600
            Requester_group=issue.fields.customfield_11016
            Customer_name=issue.fields.customfield_10800
            SLA=issue.fields.customfield_11017
            Reporting_medium=issue.fields.customfield_11015
            Variant=issue.fields.customfield_11405
            labels=issue.fields.labels
            sev2_data.append([issue.key,issue.fields.summary,starttime,endtime,systemid,affected,SE,Category,Requester_group,Customer_name,SLA,Reporting_medium,Variant,labels,'sev2'])

        df2=pd.DataFrame(sev2_data)
        df2.columns=['Id','Summary','Starttime','Endtime','Systemid','Affected','SE','Category','Requester_group','Customer_name','SLA','Reporting_medium','Variant','labels','severity']
        df=pd.concat([df1,df2])
        df['CustomerName'] = self.tenant_info['DisplayName']
        df['time'] = df['Starttime']    
        df['time'] = df['time'].apply(lambda x:x.split('.')[0].replace('T',' '))

        data = df
        data = data[data['CustomerName']==self.tenant_info["Name"]]
        data.drop(['Customer_name','Summary','Category','Requester_group','SE','Variant'],inplace=True,axis=1)
        data['time'] = pd.to_datetime(data['time'])
        data = data.set_index('time')

        if len(data)>0:
            try:
                # client = DataFrameClient(host=host, port=port, username='gor', password='Grey()range')
                # client.write_points(data, 'software_issues', database='Alteryx', protocol='line', time_precision='s')
                self.write_client = Write_InfluxData(host=self.tenant_info["write_influx_ip"],
                                                 port=self.tenant_info["write_influx_port"])
                self.write_client.writepoints(data, "software_issues", db_name=self.tenant_info["alteryx_out_db_name"],tag_columns=None, dag_name=os.path.basename(__file__), site_name=self.tenant_info['Name'])

                print(len(data),' records inserted for: ',self.tenant_info["Name"])
            except:
                print('Couldnt push data for: ',self.tenant_info["Name"])
        else:
            print('No data for: ',self.tenant_info["Name"])

        print('Script Ended')
        return None
    
with DAG(
    'software_issues',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'software issues',
    schedule_interval = timedelta(hours=24),
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
            if tenant['Active'] == "Y" and tenant['software_issues'] == "Y":
                software_issues_final_task = PythonOperator(
                    task_id='software_issues_{}'.format(tenant['Name']),
                    provide_context=True,
                    python_callable=functools.partial(SoftwareIssues().final_call,tenant_info={'tenant_info': tenant}),
                    execution_timeout=timedelta(seconds=3600),
                )
    else:
        # tenant = {"Name":"site", "Butler_ip":"dev_postgres", "influx_ip":"dev_influxdb", "influx_port":8086,\
        #           "write_influx_ip":"dev_influxdb","write_influx_port":8086, \
        #           "out_db_name":"airflow", "alteryx_out_db_name": "airflow"}
        tenant = CommonFunction().get_tenant_info()
        sotware_issues_final_task = PythonOperator(
            task_id='software_issues_final',
            provide_context=True,
            python_callable=functools.partial(SoftwareIssues().final_call,tenant_info={'tenant_info': tenant}),
            # op_kwargs={
            #     'tenant_info': tenant,
            # },
            execution_timeout=timedelta(seconds=3600),
        )
