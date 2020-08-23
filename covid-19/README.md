# COVID-19

Python Version: 3.7.7

## Quick Start

https://airflow.apache.org/docs/stable/start.html

## Starting the Webserver

```sh
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver
```

## Starting the Scheduler

```sh
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

## Starting MySQL (Docker)

```sh
docker run --name airflow-mysql -e MYSQL_ROOT_PASSWORD=root -p 3306:3306 mysql:8.0.20
```

## Creating a Database

```sql
CREATE TABLE daily_covid19_reports (
  id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  confirmed INT(6),
  recovered INT(6),
  hospitalized INT(6),
  deaths INT(6),
  new_confirmed INT(6),
  new_recovered INT(6),
  new_hospitalized INT(6),
  new_deaths INT(6),
  update_date DATETIME,
  source VARCHAR(100),
  dev_by VARCHAR(100),
  server_by VARCHAR(100)
);
```

## Querying COVID-19 Report Data

```sql
SELECT * FROM daily_covid19_reports;
```

## Starting HTTP/SMTP Server

See the instructure here: [MailHog](https://github.com/mailhog/MailHog)
