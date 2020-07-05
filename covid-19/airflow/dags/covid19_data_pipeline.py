import json
from datetime import datetime

import requests

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def get_covid19_report_today():
    url = 'https://covid19.th-stat.com/api/open/today'
    response = requests.get(url)
    data = response.json()
    with open('data.json', 'w') as f:
        json.dump(data, f)

    return data


def save_data_into_db():
    mysql_hook = MySqlHook(mysql_conn_id='covid19')
    with open('data.json') as f:
        data = json.load(f)

    insert = """
        INSERT INTO daily_covid19_reports (
            confirmed,
            recovered,
            hospitalized,
            deaths,
            new_confirmed,
            new_recovered,
            new_hospitalized,
            new_deaths,
            update_date,
            source,
            dev_by,
            server_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    mysql_hook.run(insert, parameters=(data['Confirmed'],
                                       data['Recovered'],
                                       data['Hospitalized'],
                                       data['Deaths'],
                                       data['NewConfirmed'],
                                       data['NewRecovered'],
                                       data['NewHospitalized'],
                                       data['NewDeaths'],
                                       datetime.strptime(data['UpdateDate'], '%d/%m/%Y %H:%M'),
                                       data['Source'],
                                       data['DevBy'],
                                       data['SeverBy']))


default_args = {
    'owner': 'dataength',
    'start_date': datetime(2020, 7, 1),
    'email': ['dataength@hey.team'],
}
with DAG('covid19_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for COVID-19 report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_covid19_report_today',
        python_callable=get_covid19_report_today
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    t3 = EmailOperator(
        task_id='send_email',
        to=['zkan@hey.team'],
        subject='Your COVID-19 report today is ready',
        html_content='Please check your dashboard. :)'
    )

    t1 >> t2 >> t3
