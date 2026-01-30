import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import airflow
from datetime import timedelta, datetime, timezone
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from utils.CommonFunction import CommonFunction
from utils.DagIssueAlertUtils import push_failure_to_central_influx_mails

class SiteRunningOnServer:
    
    def get_all_site_data_config(self):
        self.CommonFunction = CommonFunction()
        df_sheet = self.CommonFunction.get_all_site_data_config_sheet()
        df_sheet = df_sheet[df_sheet['Active'] == 'Y']
        return df_sheet


    def fetch_site_running_on_server(self):
        csvReader = self.get_all_site_data_config()
        sites = csvReader['Name'].tolist()
        return sites
    

    def create_email_body(self, sites):
        html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Site Running on Testing Server</title>
                </head>
                <body>
                    <h2>Alert: Site Running on Testing Server</h2>
                    <p>The following sites are running on testing server on {self.host}:</p>
                    <ul>
                        {"".join(f"<li>{site}</li>" for site in sites)}
                    </ul>
                    <p>Please check Site.</p>
                    <p>Thank you!</p>
                </body>
                </html>
                """
        return html

    def check(self):
        self.host = os.environ.get('GM_WORKER_NAME', 'dev')
        sites = self.fetch_site_running_on_server()
        exception_sites = ['GFC-1_192_168_5_70','GFC-1_192_168_5_73','ConsolidateReportStatus','ConsolidateReportStatusWeekly','Eddie_Bauer_Stg']
        sites_running = [item for item in sites if item not in exception_sites]
        if sites_running:
            current_datetime = datetime.now()
            formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            subject = formatted_datetime + f"Sites running on {self.host}"   
            reciever_email = ['analytics-team@greyorange.com']
            html_body = self.create_email_body(sites_running)
            self.CommonFunction.send_mail(html_body, subject, 'customer_success@greyorange.com', reciever_email)
            data ={"time":formatted_datetime, "subject":subject,"html_body": ", ".join(sites_running),'category':'live_site_on_testing_env'}
            push_failure_to_central_influx_mails(data)



with DAG(
    'site_running_on_server',
    default_args = CommonFunction().get_default_args_for_dag(),
    description = 'Check if sites are running on server',
    schedule_interval = '45 * * * *',
    max_active_runs = 1,
    max_active_tasks = 16,
    concurrency = 16,
    catchup = False
) as dag:
    import csv
    import os
    import functools
    final_task = PythonOperator(
        task_id='site_running_on_server_final',
        provide_context=True,
        python_callable=functools.partial(SiteRunningOnServer().check),
        execution_timeout=timedelta(seconds=3600),
    )
