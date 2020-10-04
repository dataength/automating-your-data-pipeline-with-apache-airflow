import pickle

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone

from sklearn.ensemble import RandomForestClassifier


default_args = {
  'owner': 'ODDS',
}
dag = DAG(
    'train_simple_model',
    schedule_interval='*/15 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 1),
    catchup=False
)

start = DummyOperator(task_id='start', dag=dag)


def train_func():
    clf = RandomForestClassifier(random_state=0)
    X = [[ 1,  2,  3],
         [11, 12, 13]]
    y = [0, 1]
    clf.fit(X, y)

    MODEL_PATH = '/Users/zkan/Projects/dataength/' \
        'automating-your-data-pipeline-with-apache-airflow/' \
        'machine-learning-pipeline/airflow/dags'

    with open(f'{MODEL_PATH}/models/clf.model', 'wb') as outfile:
        pickle.dump(clf, outfile)


train = PythonOperator(
    task_id='train',
    python_callable=train_func,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> train >> end
