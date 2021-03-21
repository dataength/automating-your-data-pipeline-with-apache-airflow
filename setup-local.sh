#!/bin/bash

AIRFLOW_VERSION=2.0.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install --upgrade pip==20.2.4
pip install "apache-airflow[postgres,amazon]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

pip install apache-airflow-providers-postgres
pip install apache-airflow-providers-amazon
pip install apache-airflow-providers-papermill
pip install great_expectations airflow-provider-great-expectations
