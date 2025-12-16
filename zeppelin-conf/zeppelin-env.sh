#!/bin/bash

# Spark Configuration
export SPARK_HOME=/opt/spark
export SPARK_MASTER=spark://spark-master:7077


# Hive Configuration
export HIVE_CONF_DIR=/opt/spark/conf

# Spark Submit Options
export SPARK_SUBMIT_OPTIONS="
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.sql.warehouse.dir=/user/hive/warehouse \
  --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0
"

# Memory Settings
export ZEPPELIN_MEM="-Xms1024m -Xmx1024m -XX:MaxMetaspaceSize=512m"
export ZEPPELIN_INTP_MEM="-Xms1024m -Xmx1024m -XX:MaxMetaspaceSize=512m"