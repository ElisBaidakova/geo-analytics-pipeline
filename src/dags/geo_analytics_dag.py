from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'geo_analytics_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['geo', 'spark']
) as dag:

    build_user_mart = SparkSubmitOperator(
        task_id='build_user_mart',
        application='/lessons/user_mart.py',
        conn_id='spark_default',
        name='user_mart_job',
        conf={
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g'
        }
    )

    # zone_mart.py импортирует user_mart, поэтому передаём его через py-files
    build_zone_mart = SparkSubmitOperator(
        task_id='build_zone_mart',
        application='/lessons/zone_mart.py',
        conn_id='spark_default',
        name='zone_mart_job',
        py_files='/lessons/user_mart.py',  # Передаём зависимость
        conf={
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g'
        }
    )

    # friend_rec_mart.py тоже импортирует user_mart
    build_friend_rec = SparkSubmitOperator(
        task_id='build_friend_rec_mart',
        application='/lessons/friend_rec_mart.py',
        conn_id='spark_default',
        name='friend_rec_job',
        py_files='/lessons/user_mart.py',  # Передаём зависимость
        conf={
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g'
        }
    )

    # Зависимости: сначала пользовательская витрина, потом остальные
    build_user_mart >> [build_zone_mart, build_friend_rec]