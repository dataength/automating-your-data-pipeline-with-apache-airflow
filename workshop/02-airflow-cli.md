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