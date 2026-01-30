# #################################
# # List all non-standard packages to be imported by your
# # script here (only missing packages will be installed)
# from ayx import Package

# # Package.installPackages(['pandas','numpy'])


# #################################
# Package.installPackages(['jira', 'timedelta'])

# #################################
# from ayx import Alteryx

# from jira.client import JIRA

# from jira import JIRA

# from jira.exceptions import JIRAError

# from datetime import datetime, timedelta

# from datetime import time

# import pandas as pd

# #################################
# jira_server = 'https://work.greyorange.com/jira/'

# jira_user = 'bot-analytics'

# jira_password = 'bot-analytics@123'

# jira_server = {'server': jira_server}

# jira = JIRA(options=jira_server, basic_auth=(jira_user, jira_password))

# block_size = 5

# block_num = 0

# Kal = datetime.today() - timedelta(days=1)

# Yesterday = Kal.strftime('%Y-%m-%d')

# Yes = datetime.strptime(Yesterday, '%Y-%m-%d').day

# Parso = datetime.today() - timedelta(days=120)

# week = Parso.strftime('%Y-%m-%d')

# Today = datetime.today().strftime('%Y-%m-%d')

# sev1 = jira.search_issues(
#     jql_str="project = SS AND issuetype in ('Incident  (Butler)')  AND cf[11017]=\"Breakdown (CSS-268)\"AND 'Start Date and Time'>='%s'" % week,
#     maxResults=90000)

# sev2 = jira.search_issues(
#     jql_str="project = SS AND issuetype in ('Incident  (Butler)') AND cf[11017]=\"Partial Breakdown (CSS-271)\"AND 'Start Date and Time'>='%s'" % week,
#     maxResults=90000)

# df = 0

# issue = 0

# sev1_data = []

# sev2_data = []

# for issue in sev1:
#     starttime = (issue.fields.customfield_11003)

#     endtime = (issue.fields.customfield_11010)

#     systemid = issue.fields.customfield_11005

#     affected = issue.fields.customfield_11404

#     SE = issue.fields.customfield_11018

#     Category = issue.fields.customfield_12600

#     Requester_group = issue.fields.customfield_11016

#     Customer_name = issue.fields.customfield_10800

#     SLA = issue.fields.customfield_11017

#     Reporting_medium = issue.fields.customfield_11015

#     Variant = issue.fields.customfield_11405

#     labels = issue.fields.labels

#     # comments=issue.fields.comment.comments

#     #

#     # for comment in comments:

#     #         com=("Text: ", comment.body)

#     sev1_data.append(
#         [issue.key, issue.fields.summary, starttime, endtime, systemid, affected, SE, Category, Requester_group,
#          Customer_name, SLA, Reporting_medium, Variant, labels, 'sev1'])

# df1 = pd.DataFrame(sev1_data)

# df1.columns = ['Id', 'Summary', 'Starttime', 'Endtime', 'Systemid', 'Affected', 'SE', 'Category', 'Requester_group',
#                'Customer_name', 'SLA', 'Reporting_medium', 'Variant', 'labels', 'severity']

# for issue in sev2:
#     starttime = (issue.fields.customfield_11003)

#     endtime = (issue.fields.customfield_11010)

#     systemid = issue.fields.customfield_11005

#     affected = issue.fields.customfield_11404

#     SE = issue.fields.customfield_11018

#     Category = issue.fields.customfield_12600

#     Requester_group = issue.fields.customfield_11016

#     Customer_name = issue.fields.customfield_10800

#     SLA = issue.fields.customfield_11017

#     Reporting_medium = issue.fields.customfield_11015

#     Variant = issue.fields.customfield_11405

#     labels = issue.fields.labels

#     # comments = issue.fields.comment.comments

#     #

#     # for comment in comments:

#     #         com=("Text: ", comment.body)

#     sev2_data.append(
#         [issue.key, issue.fields.summary, starttime, endtime, systemid, affected, SE, Category, Requester_group,
#          Customer_name, SLA, Reporting_medium, Variant, labels, 'sev2'])

# df2 = pd.DataFrame(sev2_data)

# df2.columns = ['Id', 'Summary', 'Starttime', 'Endtime', 'Systemid', 'Affected', 'SE', 'Category', 'Requester_group',
#                'Customer_name', 'SLA', 'Reporting_medium', 'Variant', 'labels', 'severity']

# # Final data to be considered

# df = pd.concat([df1, df2])

# #################################
# df

# #################################
# Alteryx.write(df, 1)

# #################################
