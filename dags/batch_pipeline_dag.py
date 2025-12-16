from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="exchange_rates_batch_and_verify_hdfs",
    start_date=datetime(2025, 12, 14),
    schedule_interval="@daily",
    catchup=False,
    tags=['spark', 'exchange-rates', 'hdfs']
) as dag:

    # ====================================================
    # Task 1 : Exécution du Batch Layer (Kafka -> HDFS)
    # ====================================================
    run_batch = BashOperator(
        task_id="run_batch_layer",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.hadoop.fs.defaultFS=hdfs://hadoop-namenode:8020 \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
          /opt/spark/jobs/batch_layer.py
        """
    )

    # ====================================================
    # Task 2 : Vérification des données sur HDFS
    # ====================================================
    run_verify = BashOperator(
        task_id="verify_hdfs_avro_parquet",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --conf spark.hadoop.fs.defaultFS=hdfs://hadoop-namenode:8020 \
          --packages org.apache.spark:spark-avro_2.12:3.5.0 \
          /opt/spark/jobs/verify_hdfs_data.py
        """
    )

    # ====================================================
    # Task 3 : Vérification rapide des fichiers HDFS
    # ====================================================
    check_hdfs = BashOperator(
        task_id="check_hdfs",
        bash_command="""
        echo "=== Contenu HDFS /exchange_rates ==="
        docker exec hadoop-namenode hdfs dfs -ls -R /exchange_rates
        """
    )

    # ====================================================
    # Ordre d'exécution
    # ====================================================
    run_batch >> run_verify >> check_hdfs