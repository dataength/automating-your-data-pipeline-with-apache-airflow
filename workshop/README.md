# Workshop on Automating Your Data Pipelines with Apache Airflow

Here is the [instruction](instruction.md).

## Hive Commands

Creating table with partition

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

Inserting new row with partition into table

```
set hive.exec.dynamic.partition.mode=nonstrict;
```

```sql
INSERT INTO customer_transactions PARTITION(txn_date) VALUES('123', 1860, 'Credit', '2019-04-14');
```

Inserting another new row with partition

```sql
INSERT INTO customer_transactions PARTITION(txn_date) VALUES('121', 588, 'Debit', '2019-04-14');
```

Showing partitions

```sql
SHOW PARTITIONS customer_transactions;
```

Loading data into table

File name: 2019-11-15.txt
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

Querying data

```sql
SELECT * FROM customer_transactions;
```

Querying data with partition

```sql
SELECT * FROM customer_transactions WHERE txn_date = '2019-11-15';
```

Querying then insert with partition

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