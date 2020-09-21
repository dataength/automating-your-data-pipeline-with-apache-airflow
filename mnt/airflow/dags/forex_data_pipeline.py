import csv
import json
from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.sensors.http_sensor import HttpSensor

import requests


default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@host.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def download_rates():
    with open('/usr/local/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for row in reader:
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get('https://api.exchangeratesapi.io/latest?base=' + base).json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/usr/local/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

with DAG(dag_id="forex_data_pipeline_final", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
    # Checking if forex rates are avaiable
    # TODO: Check SSL
    is_forex_rates_available = HttpSensor(
            task_id="is_forex_rates_available",
            method="GET",
            http_conn_id="forex_api",
            endpoint="latest",
            response_check=lambda response: "rates" in response.text,
            poke_interval=5,
            timeout=20
    )

    # Checking if the file containing the forex pairs we want to observe is arrived
    # TODO: Speak about the fact that the path in connection forex_path must be specified
    # in the extra parameter as JSON
    is_forex_currencies_file_available = FileSensor(
            task_id="is_forex_currencies_file_available",
            fs_conn_id="forex_path",
            filepath="forex_currencies.csv",
            poke_interval=5,
            timeout=20
    )

    # Parsing forex_pairs.csv and downloading the files
    downloading_rates = PythonOperator(
            task_id="downloading_rates",
            python_callable=download_rates
    )

    # Saving forex_rates.json in HDFS
    saving_rates = BashOperator(
        task_id="saving_rates",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
            """
    )

    # Creating a hive table named forex_rates
    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    # Running Spark Job to process the data
    forex_processing = SparkSubmitOperator(
        task_id="forex_processing",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/forex_processing.py",
        verbose=False
    )

    # Sending a notification by email
    # https://stackoverflow.com/questions/51829200/how-to-set-up-airflow-send-email
    sending_email_notification = EmailOperator(
            task_id="sending_email",
            to="airflow_course@yopmail.com",
            subject="forex_data_pipeline",
            html_content="""
                <h3>forex_data_pipeline succeeded</h3>
            """
            )

    # Sending a notification by Slack message
    # TODO: Improvements - add on_failure for tasks
    # https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
    sending_slack_notification = SlackAPIPostOperator(
        task_id="sending_slack",
        token="xoxp-753801195270-740121926339-751642514144-8391b800988bed43247926b03742459e",
        username="airflow",
        text="DAG forex_data_pipeline: DONE",
        channel="#airflow-exploit"
    )

    is_forex_rates_available >> is_forex_currencies_file_available >> downloading_rates >> saving_rates
    saving_rates >> creating_forex_rates_table >> forex_processing 
    forex_processing >> sending_email_notification >> sending_slack_notification