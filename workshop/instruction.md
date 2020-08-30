# Workshop Instruction

1. [Build Your First DAG](#build-your-first-dag)
1. [Airflow CLI](#airflow-cli)
1. [Let's Build a Data Pipeline](#lets-build-a-data-pipeline)
    * [Product Lookup Pipeline](#product-lookup-pipeline)
    * [Store Lookup Pipeline](#store-lookup-pipeline)
    * [Transaction Load Pipeline](#transaction-load-pipeline)
    * [Product Price Range Pipeline](#product-price-range-pipeline)
1. [Scaling Airflow](#scaling-airflow)

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
import logging


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

## Let's Build a Data Pipeline

What is the range of prices offered on products?

### Product Lookup Pipeline

Start with a simple DAG below. We'll be then incrementally building up our pipeline.

```py
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone


default_args = {
    'owner': 'zkan',
    'email': ['zkan@hey.com'],
}
dag = DAG(
    'product_lookup_pipeline',
    schedule_interval='@hourly',
    default_args=default_args,
    start_date=timezone.datetime(2020, 9, 12),
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)
end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> end
```

Check if the file is available yet or not.

```py
from airflow.contrib.sensors.file_sensor import FileSensor


is_file_available = FileSensor(
    task_id='is_file_available',
    fs_conn_id='my_file_conn',
    filepath='product-lookup-table.csv',
    poke_interval=5,
    timeout=100,
    dag=dag,
)
```

Add this task to the existing DAG, so it'll look like this.

```
start >> is_file_available >> end
```

Remove empty columns and create a new file.

```py
import logging

from airflow.operators.python_operator import PythonOperator

import pandas as pd


DATA_FOLDER = '/usr/local/airflow/dags/files'

def remove_empty_columns_func():
    df = pd.read_csv(f'{DATA_FOLDER}/product-lookup-table.csv', header=1)
    logging.info(df.head())
    df[[
        'UPC',
        'DESCRIPTION',
        'MANUFACTURER',
        'CATEGORY',
        'SUB_CATEGORY',
        'PRODUCT_SIZE'
    ]].to_csv(f'{DATA_FOLDER}/products-with-good-columns.csv', index=False, header=False)


remove_empty_columns = PythonOperator(
    task_id='remove_empty_columns',
    python_callable=remove_empty_columns_func,
    dag=dag,
)
```

The DAG will be:

```
start >> is_file_available >> remove_empty_columns >> end
```

Upload file to HDFS
```sh
from airflow.operators.bash_operator import BashOperator


upload_to_hdfs = BashOperator(
    task_id='upload_to_hdfs',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/products-with-good-columns.csv /products-with-good-columns.csv',
    dag=dag,
)
```

The DAG will be:

```
start >> is_file_available >> remove_empty_columns >> upload_to_hdfs >> end
```

Create a Hive table.

```py
from airflow.operators.hive_operator import HiveOperator


create_product_lookup_table = HiveOperator(
    task_id='create_product_lookup_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS product_lookup(
            UPC          STRING,
            DESCRIPTION  STRING,
            MANUFACTURER STRING,
            CATEGORY     STRING,
            SUB_CATEGORY STRING,
            PRODUCT_SIZE STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    ''',
    dag=dag,
)
```

The DAG will be:

```
start >> is_file_available >> remove_empty_columns >> upload_to_hdfs >> create_product_lookup_table >> end
```

Finally, we load data to Hive table.

```py
load_data_to_hive_table = HiveOperator(
    task_id='load_data_to_hive_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        LOAD DATA INPATH '/products-with-good-columns.csv' OVERWRITE INTO TABLE product_lookup;
    ''',
    dag=dag,
)
```

The DAG will be:

```
start >> is_file_available >> remove_empty_columns >> upload_to_hdfs >> create_product_lookup_table >> load_data_to_hive_table >> end
```

🎉

### Store Lookup Pipeline

Let's take the product lookup pipeline as an inspiration. This pipeline will look very similar to it. :)

Here is the complete DAG.

```py
import logging

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone

import pandas as pd


default_args = {
    'owner': 'zkan',
    'email': ['zkan@hey.com'],
    'sla': timedelta(seconds=30),
}
dag = DAG(
    'store_lookup_pipeline',
    schedule_interval='@hourly',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

is_file_available = FileSensor(
    task_id='is_file_available',
    fs_conn_id='my_file_path',
    filepath='store-lookup-table.csv',
    poke_interval=5,
    timeout=100,
    dag=dag,
)

DATA_FOLDER = '/usr/local/airflow/dags/files'

def remove_empty_columns_func():
    df = pd.read_csv(f'{DATA_FOLDER}/store-lookup-table.csv', header=1)
    logging.info(df.head())
    df[
        [
            'STORE_ID',
            'STORE_NAME',
            'ADDRESS_CITY_NAME',
            'ADDRESS_STATE_PROV_CODE',
            'MSA_CODE',
            'SEG_VALUE_NAME',
            'PARKING_SPACE_QTY',
            'SALES_AREA_SIZE_NUM',
            'AVG_WEEKLY_BASKETS'
        ]
    ].to_csv(f'{DATA_FOLDER}/store-with-good-columns.csv', index=False, header=False)


remove_empty_columns = PythonOperator(
    task_id='remove_empty_columns',
    python_callable=remove_empty_columns_func,
    dag=dag,
)

# Upload to HDFS
upload_to_hdfs = BashOperator(
    task_id='upload_to_hdfs',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/store-with-good-columns.csv /store-with-good-columns.csv',
    dag=dag,
)

# Create Hive table
create_store_lookup_table = HiveOperator(
    task_id='create_store_lookup_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS store_lookup (
            store_id                 INT,
            store_name               VARCHAR(100),
            address_city_name        VARCHAR(300),
            address_state_prov_code  VARCHAR(2),
            msa_code                 VARCHAR(100),
            seg_value_name           VARCHAR(100),
            parking_space_qty        DECIMAL(38, 2),
            sales_area_size_num      INT,
            avg_weekly_baskets       DECIMAL(38, 2)
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        STORED AS TEXTFILE;
    ''',
    dag=dag,
)

# Load data in Hive table
load_data_to_hive_table = HiveOperator(
    task_id='load_data_to_hive_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        LOAD DATA INPATH '/store-with-good-columns.csv' OVERWRITE INTO TABLE store_lookup;
    ''',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define DAG dependencies
start >> is_file_available >> remove_empty_columns >> upload_to_hdfs >> create_store_lookup_table
create_store_lookup_table >> load_data_to_hive_table >> end
```

🎉

### Transaction Load Pipeline

Backfill

```sh
airflow backfill -s 2009-01-01 -e 2009-02-05 --reset_dagruns transaction_load_pipeline
```

In case of having many workers, use `--donot_pickle` to not attempt to pickle the DAG object to send over to the workers. The workers will run their version of the code. This way we don't need to deal with the DAG serialization issue that may happen.

```sh
airflow backfill -s 2009-01-01 -e 2009-01-16 --donot_pickle --reset_dagruns transaction_load_pipeline
```

### Product Price Range Pipeline

## Scaling Airflow