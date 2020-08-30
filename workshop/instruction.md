# Workshop Instruction

## Build Your First DAG

Let's create a new file named `my_dag.py`.

### Importing Packages

```py
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
```

### Setting Up `default_args`

```py
default_args = {
    'owner': 'Skooldio',
}
```

### Defining DAG, Tasks, and Operators

```py
dag = DAG(
    'my_dag',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 1),
    catchup=False,
)

t1 = DummyOperator(
    task_id='my_1st_dummy_task',
    dag=dag,
)
t2 = DummyOperator(
    task_id='my_2nd_dummy_task',
    dag=dag,
)

t1 >> t2
```

### BashOperator

```py
from airflow.operators.bash_operator import BashOperator


echo_hello = BashOperator(
    task_id='echo_hello',
    bash_command='echo hello',
    dag=dag
)
```

### PythonOperator

```py
from airflow.operators.python_operator import PythonOperator


def hello():
    return 'Hello, Python'


say_hello = PythonOperator(
    task_id='say_hello',
    python_callable=hello,
    dag=dag
)
```

### Logging

```py
def print_log_messages():
    logging.debug('This is a debug message')
    logging.info('This is an info message')
    logging.warning('This is a warning message')
    logging.error('This is an error message')
    logging.critical('This is a critical message')

    return 'Whatever is returned also gets printed in the logs'


run_this = PythonOperator(
    task_id='print_log_messages',
    python_callable=print_log_messages,
    dag=dag,
)
```

## Airflow CLI

```sh
# command layout: command subcommand dag_id task_id date
airflow test tutorial print_date 2015-06-01

# print the list of active DAGs
airflow list_dags

# prints the list of tasks the "tutorial" dag_id
airflow list_tasks tutorial

# prints the hierarchy of tasks in the tutorial DAG
airflow list_tasks tutorial --tree

# testing templated
airflow test tutorial templated 2015-06-01
```