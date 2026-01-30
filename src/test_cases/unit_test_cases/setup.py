import unittest
from datetime import timedelta, datetime, timezone
import os
from airflow.models import DagBag, TaskInstance
from airflow import settings
from airflow.utils import db

# Use the testing configuration
settings.AIRFLOW_CONFIG = '/src/test_cases/airflow_test.cfg'

class TestSetupDBDag(unittest.TestCase):
    Butler_ip = os.environ.get('MNESIA_IP', '192.168.32.86')
    influx_ip = os.environ.get('INFLUX_HOSTNAME', 'localhost')
    write_influx_ip = os.environ.get('INFLUX_HOSTNAME', '10.8.1.244')
    influx_port = os.environ.get('INFLUX_PORT', '8086')
    db_name = os.environ.get('Out_db_name', 'GreyOrange')
    alteryx_out_db_name = os.environ.get('alteryx_out_db_name', 'Alteryx')
    streaming = os.environ.get('streaming', "N")
    TimeZone = os.environ.get('TimeZone', 'PDT')
    Postgres_pf_user = os.environ.get('Postgres_pf_user', "altryx_read")
    Postgres_pf_password = os.environ.get('Postgres_pf_password', "YLafmbfuji$@#45!")
    Postgres_pf = os.environ.get('Postgres_pf', 'localhost')
    Postgres_pf_port = os.environ.get('Postgres_pf_port', 5432)
    sslrootcert = os.environ.get('sslrootcert', "")
    sslcert = os.environ.get('sslcert', "")
    sslkey = os.environ.get('sslkey', "")
    Postgres_butler_port = 36304
    Postgres_ButlerDev = Butler_ip
    Postgres_butler_user = os.environ.get('Postgres_pf_user', "altryx_read")
    Postgres_butler_password = os.environ.get('Postgres_pf_password', "YLafmbfuji$@#45!")
    # DEFAULT_DATE = datetime.strptime('04/23/23 13:55:26', '%m/%d/%y %H:%M:%S')
    DEFAULT_DATE = datetime.now(timezone.utc) - timedelta(days=1)
    expected_influx_data = {
        "Name": "HnmCanada",
        "Butler_ip": Butler_ip,
        "influx_ip": influx_ip,
        "influx_port": influx_port,
        "write_influx_ip": influx_ip,
        "write_influx_port": influx_port,
        "sslcert": sslcert,
        "sslkey": sslkey,
        "sslrootcert": sslrootcert,
        "Postgres_butler_port": Postgres_butler_port,
        "Postgres_ButlerDev": Postgres_ButlerDev,
        "Postgres_butler_password": Postgres_butler_password,
        "out_db_name": db_name,
        "alteryx_out_db_name": alteryx_out_db_name,
        "Butler_port": "8181",
        "Postgres_butler_user": "altryx_read",
        "site": "test",
        "Tower_user": Postgres_butler_user,
        "Postgres_tower": "92.168.1.187",
        "Postgres_tower_port": "7093",
        "Tower_password": Postgres_butler_password,
        "elastic_ip": "10.8.1.249",
        "elastic_port": "9200",
        "StartTime":"07:00:00"
    }
    def setUp(self):
        db.initdb()
        self.dagbag = DagBag(dag_folder='.', include_examples=False)

