```python
from datetime import timedelta

from airflow.utils.dates import days_ago
```

```python
default_args = {
    'owner': 'Skooldio',
    'start_date': days_ago(2),
    # 'email': ['zkan@hey.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'sla': timedelta(hours=2),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
}
```

```python
dag = DAG(
    'your_dag_id',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    catchup=False,
)
```

```python
from airflow.macros import ds_format


new_date = ds_format(ds, '%Y-%m-%d', '%Y/%m/%d/')
```

```python
from airflow.operators.dummy_operator import DummyOperator


my_1st_dummy_task = DummyOperator(
    task_id='my_1st_dummy_task', 
    dag=dag,
)
```

```python
from airflow.operators.bash_operator import BashOperator


echo = BashOperator(
    task_id='echo',
    bash_command='echo {{ execution_date }}',
)
```

```python
from airflow.operators.python_operator import PythonOperator


run_this = PythonOperator(
    task_id='print_the_context',
    python_callable=print_context,
    # op_kwargs={'random_base': 1 / 10},
    sla=timedelta(seconds=3),
    dag=dag,
)
```

```python
from airflow.contrib.sensors.file_sensor import FileSensor


my_file_sensor_task = FileSensor(
    task_id='my_file_sensor_task',
    poke_interval=5,
    fs_conn_id='my_fs',
    filepath='data.txt',
    dag=dag,
)
```

```python
dag = DAG(
    'test_schedule',
    schedule_interval='0 0 * * THU',
    default_args=default_args,
    start_date=timezone.datetime(2009, 1, 7),
    catchup=False
)

t0 = BashOperator(
    task_id='echo',
    bash_command='echo "Get data on {{ macros.ds_add(ds, -1) }}"',
    dag=dag,
)
```

```sh
airflow backfill -s 2009-01-01 -e 2009-02-05 --reset_dagruns test_schedule
```

```python
from airflow.operators.hive_operator import HiveOperator


create_some_table = HiveOperator(
    task_id='create_some_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE EXTERNAL TABLE IF NOT EXISTS some_table(
            id    INT,
            usd   DOUBLE,
            baht  DOUBLE,
            class STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    '''
)

summarize_amount = HiveOperator(
    task_id='summarize_amount',
    hive_cli_conn_id='my_hive_conn',
    hiveconfs={'hive.exec.dynamic.partition.mode': 'nonstrict'},
    hql='''
        INSERT OVERWRITE TABLE amount_summary PARTITION (txn_date)
        SELECT txn_type,
                avg(txn_amout),
                '2020-08-12'
        FROM customer_transactions
        GROUP BY txn_type;
    ''',
    dag=dag,
)
```

```python
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor


check_named_partition = NamedHivePartitionSensor(
    task_id='check_named_partition',
    # partition_names=['zkan_product_transactions/execution_date={{ ds }}'],
    partition_names=['zkan_product_transactions/execution_date=2020-07-01'],
    metastore_conn_id='my_hive_metastore_conn',
    poke_interval=10,
    dag=dag,
)
```

```python
from airflow.operators.email_operator import EmailOperator


send_me_email = EmailOperator(
    task_id='send_me_email',
    to=['zkan@hey.team'],
    subject='Your report on {{ ds }} is ready!',
    html_content='Please check your dashboard. :)'
)
```


```python
from airflow.operators.python_operator import BranchPythonOperator


options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']
branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: random.choice(options),
    dag=dag,
)

# https://airflow.apache.org/docs/stable/concepts.html#trigger-rules
join = DummyOperator(
    task_id='join',
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag,
    )

    branching >> t >> join
```

```python
from airflow.operators.dagrun_operator import TriggerDagRunOperator


def trigger(context, dag_run_obj):
    print(context)
    dag_run_obj.payload = {'message': context['params']['message']}
    return dag_run_obj


trigger = TriggerDagRunOperator(
    dag=dag,
    task_id='test_trigger_dagrun',
    trigger_dag_id="trigger_dag_target",
    python_callable=trigger,
    params={'message': 'Hello World'}
)
```

```python
docs = '''
    ## DAG Name

    #### Purpose

    This DAG connects data from one source to another,
    performs necessary transformations,
    and creates a set of tables that can be used by analysts 

    #### Outputs

    This pipeline produces the following output tables:

    - `table_A` – Contains useful information about ABC.
    - `table_b` – Contains useful inormation about XYZ.

    #### Owner

    For any questions or concerns, please contact 
    [me@mycompany.com](mailto:me@mycompany.com).
'''

dag.doc_md = docs

t0 = DummyOperator(
    task_id='t0', 
    dag=dag,
)
t0.doc_md = '''
    #### Task Documentation
    Write something here.
'''
```