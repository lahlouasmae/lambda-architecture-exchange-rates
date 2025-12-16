from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import (
    StructType, StructField, StringType,
    MapType, DoubleType
)

# 1️⃣ Créer la session Spark
spark = SparkSession.builder \
    .appName("ExchangeRatesSpeedLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️⃣ Schéma du message Kafka
schema = StructType([
    StructField("base", StringType(), True),
    StructField("date", StringType(), True),
    StructField("rates", MapType(StringType(), DoubleType()), True),
    StructField("ingestion_time", StringType(), True)
])

# 3️⃣ Lecture depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "exchange-rates") \
    .option("startingOffsets", "latest") \
    .load()

# 4️⃣ Conversion value (bytes → JSON)
json_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
)

# 5️⃣ Exploser les taux de change
rates_df = json_df.select(
    col("data.base").alias("base"),
    col("data.date").alias("rate_date"),
    col("data.ingestion_time"),
    explode(col("data.rates")).alias("currency", "rate")
)

# 6️⃣ Afficher dans la console ET stocker en mémoire
query_console = rates_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query_memory = rates_df.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("exchange_rates_speed") \
    .start()

# Attendre les deux queries
query_console.awaitTermination()