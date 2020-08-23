from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'zkan',
    'start_date': days_ago(2),
    'provide_context': True,
}
with DAG('try_ssh_operator',
         schedule_interval='@once',
         default_args=args) as dag:

    # ssh -i dataops.pem ubuntu@3.0.51.178 'cd airflow-section-3 && docker-compose exec -T namenode bash -c "hdfs dfs -put /root/iris.csv /"'
    command = '''
        cd airflow-section-3 && docker-compose exec -T namenode bash -c "hdfs dfs -put /root/iris.csv /"
    '''
    t0 = SSHOperator(
        task_id='run_ssh',
        ssh_conn_id='dataength',
        command=command,
    )

    t0
