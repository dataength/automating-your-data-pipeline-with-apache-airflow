# Commands Used in Workshop

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

### Insert new row into table

```sql
INSERT INTO customer_transactions PARTITION(txn_date) VALUES('123', 1860, 'Credit', '2019-04-14');
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

### Query then insert

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

## Building a Data Pipeline

### Product Lookup

Select only needed columns using Pandas

```python
products[
    ['UPC', 'DESCRIPTION', 'MANUFACTURER', 'CATEGORY', 'SUB_CATEGORY', 'PRODUCT_SIZE']
].to_csv('mnt/airflow/dags/products-lookup-table-cleaned.csv', index=False, header=False)
```

Load data onto HDFS

```sh
hdfs dfs -put -f products-lookup-table-cleaned.csv /products-lookup-table-cleaned.csv
```

Create Hive table and load data into it

```sql
CREATE TABLE IF NOT EXISTS product_lookup (
  upc          VARCHAR(100),
  description  VARCHAR(300),
  manufacturer VARCHAR(100),
  category     VARCHAR(100),
  sub_category VARCHAR(100),
  product_size DECIMAL(38, 2)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
```

```sql
LOAD DATA INPATH '/products-lookup-table-cleaned.csv' OVERWRITE INTO TABLE product_lookup;
```

### Store Lookup

Select only needed columns using Pandas

```python
stores[
    ['STORE_ID', 'STORE_NAME', 'ADDRESS_CITY_NAME', 'ADDRESS_STATE_PROV_CODE', 'MSA_CODE', 'SEG_VALUE_NAME', 'PARKING_SPACE_QTY', 'SALES_AREA_SIZE_NUM', 'AVG_WEEKLY_BASKETS']
].to_csv('mnt/airflow/dags/store-lookup-table-cleaned.csv', index=False, header=False)
```

Load data onto HDFS

```sh
hdfs dfs -put -f store-lookup-table-cleaned.csv /store-lookup-table-cleaned.csv
```

Create Hive table and load data into it

```sql
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
```

```sql
LOAD DATA INPATH '/store-lookup-table-cleaned.csv' OVERWRITE INTO TABLE store_lookup;
```

```sql
SELECT * FROM store_lookup WHERE parking_space_qty IS NOT NULL;
```

### Transactions

Convert week end date to date using Pandas

```python
pd.to_datetime(transactions.WEEK_END_DATE, format='%d-%b-%y')
```

Select only needed columns using Pandas

```python
transactions[
    ['WEEK_END_DATE', 'STORE_NUM', 'UPC', 'UNITS', 'VISITS', 'HHS', 'SPEND', 'PRICE', 'BASE_PRICE', 'FEATURE', 'DISPLAY', 'TPR_ONLY']
].to_csv('mnt/airflow/dags/transactions-cleaned.csv', index=False, header=False)
```

Load data onto HDFS

```sh
hdfs dfs -put -f transactions-cleaned.csv /transactions-cleaned.csv
```

Create Hive table and load data into it

```sql
CREATE TABLE IF NOT EXISTS transactions (
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
```

```sql
LOAD DATA INPATH '/transactions-cleaned.csv' OVERWRITE INTO TABLE transactions PARTITION(execution_date=date'2019-11-15');
```

## Find Answers

```sql
CREATE TABLE IF NOT EXISTS zkan_product_transactions (
  product_description STRING,
  price               DECIMAL(10, 2),
  units               INT,
  visits              INT
)
PARTITIONED BY (execution_date DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
```

```sql
INSERT INTO TABLE zkan_product_transactions
SELECT product_lookup.description,
       transactions.price,
       transactions.units,
       transactions.visits,
       transactions.execution_date
FROM transactions
JOIN product_lookup ON transactions.upc = product_lookup.upc
WHERE transactions.execution_date = '2019-11-15'
```

```sql
SELECT * FROM zkan_product_transactions;
```