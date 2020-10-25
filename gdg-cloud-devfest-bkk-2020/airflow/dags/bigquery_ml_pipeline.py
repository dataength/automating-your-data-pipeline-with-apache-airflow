from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'zkan',
    'start_date': days_ago(1),
}
with DAG(
    'bigquery_ml_pipeline',
    schedule_interval='0 0 * * 1',
    default_args=args,
    catchup=False
) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    train_model = BigQueryOperator(
        task_id='train_model',
        sql='''
            CREATE OR REPLACE MODEL `gdg_cloud_devfest_bkk_2020.purchase_prediction_model_{{ ds }}`
            OPTIONS(model_type='logistic_reg') AS
            SELECT
                IF(totals.transactions IS NULL, 0, 1) AS label,
                IFNULL(device.operatingSystem, "") AS os,
                device.isMobile AS is_mobile,
                IFNULL(geoNetwork.country, "") AS country,
                IFNULL(totals.pageviews, 0) AS pageviews
            FROM
                `bigquery-public-data.google_analytics_sample.ga_sessions_*`
            WHERE
                _TABLE_SUFFIX BETWEEN '20160801' AND '20170630'
        ''',
        # destination_dataset_table='gdg_cloud_devfest_bkk_2020.purchase_prediction_model',
        # write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id='gdg_cloud_devfest_bkk_2020',
        dag=dag
    )

    get_model_training_statistics = BigQueryOperator(
        task_id='get_model_training_statistics',
        sql='''
            CREATE OR REPLACE TABLE
                `gdg_cloud_devfest_bkk_2020.purchase_prediction_model_training_statistics_{{ ds }}` AS
            SELECT
                *
            FROM
                ML.TRAINING_INFO(MODEL `gdg_cloud_devfest_bkk_2020.purchase_prediction_model_{{ ds }}`)
            ORDER BY
                iteration DESC
        ''',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id='gdg_cloud_devfest_bkk_2020',
        dag=dag
    )

    evaluate_model = BigQueryOperator(
        task_id='evaluate_model',
        sql='''
            CREATE OR REPLACE TABLE
                `gdg_cloud_devfest_bkk_2020.purchase_prediction_model_evaluation_{{ ds }}` AS
            SELECT
                *
            FROM ML.EVALUATE(MODEL `gdg_cloud_devfest_bkk_2020.purchase_prediction_model_{{ ds }}`, (
                SELECT
                    IF(totals.transactions IS NULL, 0, 1) AS label,
                    IFNULL(device.operatingSystem, "") AS os,
                    device.isMobile AS is_mobile,
                    IFNULL(geoNetwork.country, "") AS country,
                    IFNULL(totals.pageviews, 0) AS pageviews
                FROM
                    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE
                    _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'))
        ''',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id='gdg_cloud_devfest_bkk_2020',
        dag=dag
    )

    compute_roc = BigQueryOperator(
        task_id='compute_roc',
        sql='''
            CREATE OR REPLACE TABLE
                `gdg_cloud_devfest_bkk_2020.purchase_prediction_model_roc_{{ ds }}` AS
            SELECT
                *
            FROM
                ML.ROC_CURVE(MODEL `gdg_cloud_devfest_bkk_2020.purchase_prediction_model_{{ ds }}`)
        ''',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id='gdg_cloud_devfest_bkk_2020',
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    start >> train_model >> [get_model_training_statistics, evaluate_model, compute_roc] >> end
