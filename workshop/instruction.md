# Workshop Instruction

1. [Build Your First DAG](#build-your-first-dag)
1. [Airflow CLI](#airflow-cli)
1. [Playing Around with HDFS](#playing-around-with-hdfs)
1. [Getting Familiar with Hive](#getting-familiar-with-hive)
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

## Playing Around with HDFS

### Listing the directory on HDFS

```sh
hdfs dfs -ls /
```

### Uploading a file to HDFS

```sh
hdfs dfs -put data.txt /
```

### Getting the basic file system information and statistics about the HDFS

```sh
hdfs dfsadmin -report
```

## Getting Familiar with Hive

### Create table with partition

```sql
CREATE TABLE IF NOT EXISTS customer_transactions (
  customer_id VARCHAR(40),
  txn_amount DECIMAL(38, 2),
  txn_type  VARCHAR(100)
)
PARTITIONED BY (txn_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
```

### Insert new row with partition into table

```
set hive.exec.dynamic.partition.mode=nonstrict;
```

```sql
INSERT INTO customer_transactions PARTITION(txn_date) VALUES('123', 1860, 'Credit', '2019-04-14');
```

Insert another new row with partition

```sql
INSERT INTO customer_transactions PARTITION(txn_date) VALUES('121', 588, 'Debit', '2019-04-14');
```

### Show partitions

```sql
SHOW PARTITIONS customer_transactions;
```

### Load data into table

2019-11-15.txt
```
120|2500|Credit
121|1050.50|Credit
122|100|Debit
122|500|Credit
123|100|Debit
```

Appending

```sql
LOAD DATA INPATH '/2019-11-15.txt' INTO TABLE customer_transactions PARTITION(txn_date='2019-11-15');
```

Overwriting

```sql
LOAD DATA INPATH '/2019-11-15.txt' OVERWRITE INTO TABLE customer_transactions PARTITION(txn_date='2019-11-15');
```

### Query data

```sql
SELECT * FROM customer_transactions;
```

```sql
SELECT * FROM customer_transactions WHERE txn_date = '2019-11-15';
```

### Query then insert with partition

```sql
CREATE TABLE IF NOT EXISTS amount_summary (
 txn_type     VARCHAR(100),
 total_amount DECIMAL(38, 2)
)
PARTITIONED BY (txn_date DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
```

```sql
INSERT INTO TABLE amount_summary
SELECT txn_type, avg(txn_amout) from customer_transactions GROUP BY txn_type;
```

```sql
INSERT OVERWRITE TABLE amount_summary
SELECT txn_type, avg(txn_amout) from customer_transactions GROUP BY txn_type;
```

```sql
INSERT OVERWRITE TABLE amount_summary PARTITION (txn_date)
SELECT txn_type, avg(txn_amout), '2020-08-12' from customer_transactions GROUP BY txn_type;
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

We expect that the product dataset is already stored in the HDFS, so we are going to download it onto our local machine.

```py
from airflow.operators.bash_operator import BashOperator


DATA_FOLDER = '/usr/local/airflow/dags/files'
LANDING_ZONE = '/landing'
CLEANED_ZONE = '/cleaned'

# Download data from HDFS
download_data_to_local = BashOperator(
    task_id='download_data_to_local',
    bash_command=f'hdfs dfs -get -f {LANDING_ZONE}/product-lookup-table-original.csv {DATA_FOLDER}/product-lookup-table.csv',
    dag=dag,
)
```

Add this task to the DAG:

```
start >> download_data_to_local >> end
```

Next we check if the file is available for further processing yet or not.

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
start >> download_data_to_local >> is_file_available >> end
```

Remove empty columns and create a new file.

```py
import logging

from airflow.operators.python_operator import PythonOperator

import pandas as pd


DATA_FOLDER = '/usr/local/airflow/dags/files'

def _remove_empty_columns():
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
    python_callable=_remove_empty_columns,
    dag=dag,
)
```

The DAG will be:

```
start >> download_data_to_local >> is_file_available >> remove_empty_columns >> end
```

Upload the processed file to HDFS.
```sh
upload_to_cleaned_zone = BashOperator(
    task_id='upload_to_cleaned_zone',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/products-with-good-columns.csv {CLEANED_ZONE}/products-with-good-columns.csv',
    dag=dag,
)
```

The DAG will be:

```
start >> download_data_to_local >> is_file_available >> remove_empty_columns >> upload_to_cleaned_zone >> end
```

Create a Hive table.

```py
from airflow.operators.hive_operator import HiveOperator


create_product_lookup_table = HiveOperator(
    task_id='create_product_lookup_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS dim_product_lookup (
            upc           VARCHAR(100),
            description   VARCHAR(300),
            manufacturer  VARCHAR(100),
            category      VARCHAR(100),
            sub_category  VARCHAR(100),
            product_size  DECIMAL(38, 2)
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        STORED AS TEXTFILE
    ''',
    dag=dag,
)
```

The DAG will be:

```
start >> download_data_to_local >> is_file_available >> remove_empty_columns >> upload_to_cleaned_zone >> create_product_lookup_table >> end
```

Finally, we load data to Hive table.

```py
load_data_to_hive_table = HiveOperator(
    task_id='load_data_to_hive_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        LOAD DATA INPATH '/products-with-good-columns.csv' OVERWRITE INTO TABLE dim_product_lookup;
    ''',
    dag=dag,
)
```

The DAG will be:

```
start >> download_data_to_local >> is_file_available >> remove_empty_columns >> upload_to_cleaned_zone >> create_product_lookup_table >> load_data_to_hive_table >> end
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
    fs_conn_id='my_file_conn',
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
        CREATE TABLE IF NOT EXISTS dim_store_lookup (
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
        LOAD DATA INPATH '/store-with-good-columns.csv' OVERWRITE INTO TABLE dim_store_lookup;
    ''',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define DAG dependencies
start >> is_file_available >> remove_empty_columns >> upload_to_hdfs >> create_store_lookup_table >> load_data_to_hive_table >> end
```

🎉

### Transaction Load Pipeline

Again, let's start with a simple DAG.

```py
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone


default_args = {
    'owner': 'zkan',
    'email': ['zkan@hey.com'],
}
dag = DAG(
    'transaction_load_pipeline',
    schedule_interval='0 0 * * THU', # We can then use {{ macros.ds_add(ds, -1) }}
    default_args=default_args,
    start_date=timezone.datetime(2009, 1, 1),
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

Here we want to simulate a scenario in which we are trying to extract the data from a data source by querying by week end date. In this workshop, since we don't have an actual database or some kind of system, we'll then use a dataset file `transaction-data-table` as a data source.

Let's query transaction data by week end date.

```py
from airflow import macros
from airflow.operators.python_operator import PythonOperator

import pandas as pd


DATA_FOLDER = '/usr/local/airflow/dags/files'

def query_data_by_week_end_date_func(ds):
    week_end_date = macros.ds_format(ds, '%Y-%m-%d', '%d-%b-%y')

    df = pd.read_csv(f'{DATA_FOLDER}/transaction-data-table.csv', header=1)
    new_df = df[df.WEEK_END_DATE == week_end_date]
    new_df.to_csv(f'{DATA_FOLDER}/transaction-{ds}.csv', index=False, header=True)


query_data_by_week_end_date = PythonOperator(
    task_id='query_data_by_week_end_date',
    python_callable=query_data_by_week_end_date_func,
    op_args=['{{ macros.ds_add(ds, -1) }}'],
    dag=dag,
)
```

As seen above, we use Airflow Macros and use template to deal with the date stamp. This is very powerful feature of Airflow that allows us to create a dynamic data pipeline.

We then add this task to our pipeline, so it will look like this.

```py
start >> query_data_by_week_end_date >> end
```

We could test this task with the Airflow CLI below.

```sh
airflow test transaction_load_pipeline query_data_by_week_end_date 2009-01-15
```

After run the command above, we should see a new file named `transaction-2009-01-15.csv` under the folder `dags/files`. This file should contain the transaction data on Jan 14, 2009.

Let's continue by removing the empty columns.

```py
import logging


def remove_empty_columns_func(ds):
    df = pd.read_csv(f'{DATA_FOLDER}/transaction-{ds}.csv')
    logging.info(df.head())
    df[
        [
            'WEEK_END_DATE',
            'STORE_NUM',
            'UPC',
            'UNITS',
            'VISITS',
            'HHS',
            'SPEND',
            'PRICE',
            'BASE_PRICE',
            'FEATURE',
            'DISPLAY',
            'TPR_ONLY'
        ]
    ].to_csv(f'{DATA_FOLDER}/transaction-cleaned-{ds}.csv', index=False, header=False)


remove_empty_columns = PythonOperator(
    task_id='remove_empty_columns',
    python_callable=remove_empty_columns_func,
    op_args=['{{ macros.ds_add(ds, -1) }}'],
    dag=dag,
)
```

Upload the file to HDFS.

```py
from airflow.operators.bash_operator import BashOperator


upload_to_hdfs = BashOperator(
    task_id='upload_to_hdfs',
    bash_command=f'hdfs dfs -put -f {DATA_FOLDER}/transaction-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv /transaction-cleaned-{{{{ macros.ds_add(ds, -1) }}}}.csv',
    dag=dag,
)
```

Create a Hive table with a partition. Note that this is different from what we did for the product lookup table and store lookup table. Here we need to partition the data. Therefore, when we query data with partition, it won't load the entire data.

```py
from airflow.operators.hive_operator import HiveOperator


create_transations_table = HiveOperator(
    task_id='create_transations_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS fact_transactions (
            week_end_date VARCHAR(40),
            store_num     INT,
            upc           VARCHAR(100),
            units         INT,
            visits        INT,
            hhs           INT,
            spend         DECIMAL(38, 2),
            price         DECIMAL(38, 2),
            base_price    DECIMAL(38, 2),
            feature       INT,
            display       INT,
            tpr_only      INT
        )
        PARTITIONED BY (execution_date DATE)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        STORED AS TEXTFILE;
    ''',
    dag=dag,
)
```

Load data in the Hive table with partition.

```py
load_data_to_hive_table = HiveOperator(
    task_id='load_data_to_hive_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        LOAD DATA INPATH '/transaction-cleaned-{{ macros.ds_add(ds, -1) }}.csv' OVERWRITE INTO TABLE fact_transactions PARTITION(execution_date=date'{{ macros.ds_add(ds, -1) }}');
    ''',
    dag=dag,
)
```

The final DAG will look like:

```py
start >> query_data_by_week_end_date >> remove_empty_columns >> upload_to_hdfs >> create_transations_table >> load_data_to_hive_table >> end
```

Backfill will play an important role here. We can use it to get the data stored in the past and continue getting the data in the future without modifying our DAG. Let's try the command below.

```sh
airflow backfill -s 2009-01-10 -e 2009-01-16 --reset_dagruns transaction_load_pipeline
```

In case of having many workers, use `--donot_pickle` to not attempt to pickle the DAG object to send over to the workers. The workers will run their version of the code. This way we don't need to deal with the DAG serialization issue that may happen.

```sh
airflow backfill -s 2009-01-10 -e 2009-01-16 --donot_pickle --reset_dagruns transaction_load_pipeline
```

🎉

### Product Price Range Pipeline

Let's think about this for a moment. Now we have all the data that allow us to answer the question "What is the range of prices offered on products?". We can actually create a simple join between the product lookup table and the transactions table and see the range of prices on each product. That sounds simple, right? What if we want to do it every week end date? How do we create a data pipeline that does the job? How can we automate the tasks, so that we can see the updated report every week end date? We'll find out in this section. :)

Let's start with a simple data pipeline.

```py
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone


default_args = {
    'owner': 'zkan',
    'email': ['zkan@hey.com'],
}
dag = DAG(
    'product_price_range_pipeline',
    schedule_interval='0 0 * * THU',
    default_args=default_args,
    start_date=timezone.datetime(2020, 8, 15),
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

Here comes the tricky part. This is where we want to process the data as soon as the transaction data are available in our data warehouse (Hive). This also means that as soon as we have a new partition created in the Transaction Load Pipeline above, this pipeline should be automatically run. In this workshop, we'll use `NamedHivePartitionSensor` to keep monitoring a new partition.

```py
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor


check_named_partition = NamedHivePartitionSensor(
    task_id='check_named_partition',
    partition_names=['fact_transactions/execution_date={{ macros.ds_add(ds, -1) }}'],
    metastore_conn_id='my_hive_metastore_conn',
    poke_interval=30,
    dag=dag,
)
```

We'll then create a table to store the results.

```py
from airflow.operators.hive_operator import HiveOperator


create_product_transactions_table = HiveOperator(
    task_id='create_product_transactions_table',
    hive_cli_conn_id='my_hive_conn',
    hql='''
        CREATE TABLE IF NOT EXISTS zkan_product_transactions (
            product_description STRING,
            price               DECIMAL(10, 2),
            units               INT,
            visits              INT
        )
        PARTITIONED BY (execution_date DATE)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
        STORED AS TEXTFILE;
    ''',
    dag=dag,
)
```

Now we join the two tables (Product Lookup and Transactions) and insert data in the new table we created above.

```py
add_new_product_transactions = HiveOperator(
    task_id='add_new_product_transactions',
    hive_cli_conn_id='my_hive_conn',
    hiveconfs={'hive.exec.dynamic.partition.mode': 'nonstrict'},
    hql='''
        INSERT INTO TABLE zkan_product_transactions
        SELECT dim_product_lookup.description,
            fact_transactions.price,
            fact_transactions.units,
            fact_transactions.visits,
            fact_transactions.execution_date
        FROM fact_transactions
        JOIN dim_product_lookup ON fact_transactions.upc = dim_product_lookup.upc
        WHERE fact_transactions.execution_date = '{{ macros.ds_add(ds, -1) }}'
    ''',
    dag=dag,
)
```

The final DAG will look like this.

```py
start >> check_named_partition >> create_product_transactions_table >> add_new_product_transactions >> end
```

Let's test it with the backfill command:

```sh
airflow backfill -s 2009-01-10 -e 2009-01-16 --reset_dagruns product_price_range_pipeline
```

That's it! To answer the question "What is the range of prices offered on products?", we can group the data by the product description and find min and max prices to get the range of prices of each product.

Congratulations! We've just built a complex data pipeline and should have enough knowledge to build more complex ones. Have fun! :)

🎉

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
