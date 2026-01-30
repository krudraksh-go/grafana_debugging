import os

####################################### COMMON ##########################################

SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_EMAIL = os.environ.get("SMTP_EMAIL", "")
CENTRAL_INFLUX_USERNAME = os.environ.get('CENTRAL_INFLUX_USERNAME', 'altryx')
CENTRAL_INFLUX_PASSWORD = os.environ.get('CENTRAL_INFLUX_PASSWORD', 'Z20tcH!vZC$jb3V&wYW$ZGFlZ3U')
CONFFILES_DIR = '/opt/airflow/conf_files'
HOST = os.environ.get('GM_WORKER_NAME', 'dev')
GOOGLE_TENANTS_FILE=HOST+'_gtenants.csv'
GOOGLE_ERROR_FILE='errorflag.csv'
GLOBAL_SITE_HARDWARE_FILE=HOST+'_hardware_detail.csv'
AIRFLOW_SETUP_SHEET_FILE=HOST+'_airflow_setup.xlsx'
GOOGLE_Warehouse_Manager_FILE='WM_file.csv'
GOOGLE_Warehouse_Manager_site_ops_houe_FILE='WM_file_site_ops_hr.csv'
GOOGLE_TENANTS_FILE_PATH = os.path.join(CONFFILES_DIR, GOOGLE_TENANTS_FILE)
GOOGLE_ERROR_FILE_PATH = os.path.join(CONFFILES_DIR, 'errorflag.csv')
GLOBAL_SITE_HARDWARE_FILE_PATH = os.path.join(CONFFILES_DIR,GLOBAL_SITE_HARDWARE_FILE)
AIRFLOW_SETUP_SHEET_FILE_PATH = os.path.join(CONFFILES_DIR,AIRFLOW_SETUP_SHEET_FILE)
GOOGLE_Warehouse_Manager_FILE_PATH = os.path.join(CONFFILES_DIR, 'WM_file.csv')
GOOGLE_Warehouse_Manager_site_ops_houe_FILE_PATH = os.path.join(CONFFILES_DIR, 'WM_file_site_ops_hr.csv')
INFLUX_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
GOOGLE_AUTH_JSON = os.path.join(CONFFILES_DIR, 'gor-simulation-creds.json')
GOOGLE_BUCKET_AUTH_JSON = os.path.join(CONFFILES_DIR, 'gm-prod-analytics-418c52aafb45.json')
GOOGLE_BUCKET_NAME="tempdahboard"
DJANGO_SERVER = '10.11.9.10'
############################ SODIMAC COLOMBIA MONTHLY REPORT #############################

SODIMAC_INFLUX_IP = os.environ.get("SODIMAC_INFLUX_IP", "192.168.32.86")
SODIMAC_INFLUX_PORT = os.environ.get("SODIMAC_INFLUX_PORT", "30014")
SODIMAC_INFLUX_DB = os.environ.get("SODIMAC_INFLUX_DB", "GreyOrange")
SODIMAC_SENDER_EMAIL = os.environ.get("SODIMAC_SENDER_EMAIL", "")
SODIMAC_RECEIVER_EMAIL = os.environ.get("SODIMAC_RECEIVER_EMAIL", "")

MongoDbServer=os.environ.get("MongoDbServer", "mongodb://10.11.9.10:27017/")
KafkaServer=os.environ.get("KafkaServer", "10.11.9.10:9092")
################################### GXO MONTHLY REPORT ###################################



################################### PPS STRATEGY AYX ###################################
PPS_STRATEGY_AYX_SHEET_ID = '16p8Ea7timg4OLO3CdbMhk71W_PAQJhtcb6ZoVyhYYFE'
rp_seven_days = 'rp_seven_days'
rp_one_year = 'rp_one_year'