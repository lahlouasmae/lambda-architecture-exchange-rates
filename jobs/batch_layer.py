from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import (
    StructType, StructField,
    StringType, MapType, DoubleType
)

# =========================================================
# 1. Spark Session (HDFS uniquement, pas Hive)
# =========================================================
spark = SparkSession.builder \
    .appName("ExchangeRatesBatchLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# 2. Schéma du message Kafka (JSON)
# =========================================================
schema = StructType([
    StructField("base", StringType(), True),
    StructField("date", StringType(), True),
    StructField("rates", MapType(StringType(), DoubleType()), True),
    StructField("ingestion_time", StringType(), True)
])

# =========================================================
# 3. Lecture Kafka en mode BATCH
# =========================================================
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "exchange-rates") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# =========================================================
# 4. Parsing JSON + explode
# =========================================================
json_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
)

rates_df = json_df.select(
    col("data.base").alias("base_currency"),
    explode(col("data.rates")).alias("target_currency", "rate"),
    col("data.date").alias("rate_date"),
    col("data.ingestion_time").alias("timestamp")
)

# =========================================================
# 5. Écriture MASTER DATA en AVRO dans HDFS
# =========================================================
hdfs_base_path = "hdfs://hadoop-namenode:8020/user/spark/exchange_rates"

rates_df.write \
    .mode("append") \
    .format("avro") \
    .save(f"{hdfs_base_path}/master")

print("✅ Master Data écrite dans HDFS (AVRO)")

# =========================================================
# 6. Création de la BATCH VIEW (moyenne par jour)
# =========================================================
daily_avg_df = rates_df.groupBy(
    "target_currency", "rate_date"
).avg("rate").withColumnRenamed("avg(rate)", "avg_rate")

# =========================================================
# 7. Écriture BATCH VIEW en PARQUET dans HDFS
# =========================================================
daily_avg_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{hdfs_base_path}/daily_avg")

print("✅ Batch View écrite dans HDFS (PARQUET)")

# =========================================================
# 8. Fin du job
# =========================================================
spark.stop()
print("🚀 Batch Layer terminé avec succès")