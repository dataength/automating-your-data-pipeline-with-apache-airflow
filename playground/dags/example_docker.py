from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'provide_context': True,
}
with DAG('example_docker', schedule_interval="@once", default_args=args) as dag:
    t0 = BashOperator(
        task_id='print_hello',
        bash_command='echo "hello"',
    )

    t1 = DockerOperator(
        task_id='docker_command',
        image='ubuntu:latest',
        api_version='auto',
        # auto_remove=True,
        command='bash -c "echo Hello, Docker!"',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    t0 >> t1
