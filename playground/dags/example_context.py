from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'provide_context': True,
}

dag = DAG('example_context', schedule_interval="@once", default_args=args)

value_1 = [1, 2, 3]
value_2 = {'a': 'b'}


def push(**kwargs):
    """Pushes an XCom without a specific target"""
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)


def push_by_returning(**kwargs):
    """Pushes an XCom without a specific target, just by returning it"""
    return value_2


def puller(**kwargs):
    ti = kwargs['ti']

    # get value_1
    v1 = ti.xcom_pull(key=None, task_ids='push')
    assert v1 == value_1

    # get value_2
    v2 = ti.xcom_pull(task_ids='push_by_returning')
    assert v2 == value_2

    # get both value_1 and value_2
    v1, v2 = ti.xcom_pull(key=None, task_ids=['push', 'push_by_returning'])
    assert (v1, v2) == (value_1, value_2)


def print_context(random_base, ds, **kwargs):
    print(random_base)
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',
    # provide_context=True,
    python_callable=print_context,
    op_kwargs={'random_base': 1 / 10},
    dag=dag,
)


def fn(dag=dag, number=99, **context):
    print(dag)
    print(number)


run_this_too = PythonOperator(
    task_id='print_op_kwargs',
    op_kwargs={'number': 1},
    python_callable=fn,
    dag=dag,
)

push1 = PythonOperator(
    task_id='push',
    dag=dag,
    python_callable=push,
)

push2 = PythonOperator(
    task_id='push_by_returning',
    dag=dag,
    python_callable=push_by_returning,
)

pull = PythonOperator(
    task_id='puller',
    dag=dag,
    python_callable=puller,
)

run_this >> run_this_too >> pull << [push1, push2]
