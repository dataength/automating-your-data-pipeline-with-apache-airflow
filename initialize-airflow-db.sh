#!/bin/bash

docker exec airflow bash -c "airflow db reset -y"
docker exec airflow bash -c "airflow users create --username admin --password admin --firstname Admin --lastname Admin --role Admin --email admin@example.org"