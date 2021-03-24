## Scaling Airflow

We will use the DAG below to see the results when we scale the Airflow.

```py
import time

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone


def get_sleep():
    time.sleep(10)


default_args = {
    'owner': 'zkan'
}
dag = DAG(
    'test_scaling',
    schedule_interval='*/30 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2020, 9, 12),
    catchup=False
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)
t1 = PythonOperator(
    task_id='t1',
    python_callable=get_sleep,
    dag=dag,
)
t2 = PythonOperator(
    task_id='t2',
    python_callable=get_sleep,
    dag=dag,
)
t3 = PythonOperator(
    task_id='t3',
    python_callable=get_sleep,
    dag=dag,
)
t4 = PythonOperator(
    task_id='t4',
    python_callable=get_sleep,
    dag=dag,
)
t5 = PythonOperator(
    task_id='t5',
    python_callable=get_sleep,
    dag=dag,
)
end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> [t1, t2, t3, t4, t5] >> end
```

### SequentialExecutor

```sh
docker-compose -f docker-compose.airflow-only.yml up -d
```

After finishing this section, run:

```sh
docker-compose -f docker-compose.airflow-only.yml down
```

Note that if we'll still running some Docker compose stack, let's stop it first by running this command below.

```sh
docker-compose -f docker-compose.airflow-<suffix>.yml down
```

For example, if we're running Airflow with Hive, run:

```sh
docker-compose -f docker-compose.airflow-hive.yml down
```

### LocalExecutor

```sh
docker-compose -f docker-compose.airflow-scaling-local.yml up -d
```

After finishing this section, run:

```sh
docker-compose -f docker-compose.airflow-scaling-local.yml down
```

### CeleryExecutor

```sh
docker-compose -f docker-compose.airflow-scaling-celery.yml up -d
```

We can monitor the workers and tasks in the Flower which is running at port 5555.

After finishing this section, run:

```sh
docker-compose -f docker-compose.airflow-scaling-celery.yml down
```

🎉
