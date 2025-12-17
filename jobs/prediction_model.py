from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, to_date, when
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# =========================================================
# 1. Spark Session
# =========================================================
spark = SparkSession.builder \
    .appName("ExchangeRatesMLPredictor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# 2. Lecture des données depuis HDFS
# =========================================================
HDFS_BASE = "hdfs://hadoop-namenode:8020/user/spark/exchange_rates"

master_df = spark.read.format("avro").load(f"{HDFS_BASE}/master")
print(f"📊 Données chargées: {master_df.count()}")

# =========================================================
# 3. Feature Engineering
# =========================================================
master_df = master_df.withColumn("rate_date", to_date(col("rate_date")))

window_spec = Window.partitionBy("target_currency").orderBy("rate_date")

features_df = master_df \
    .withColumn("rate_lag_1", lag("rate", 1).over(window_spec)) \
    .withColumn("rate_lag_3", lag("rate", 3).over(window_spec)) \
    .withColumn("rate_lag_7", lag("rate", 7).over(window_spec)) \
    .withColumn("rate_change", col("rate") - lag("rate", 1).over(window_spec)) \
    .withColumn(
        "rate_pct_change",
        when(lag("rate", 1).over(window_spec) != 0,
             (col("rate") - lag("rate", 1).over(window_spec)) /
             lag("rate", 1).over(window_spec) * 100
        ).otherwise(0)
    ) \
    .na.drop()

top_currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD"]
features_df = features_df.filter(col("target_currency").isin(top_currencies))

print(f"✅ Features créées: {features_df.count()}")

# =========================================================
# 4. Dataset ML
# =========================================================
feature_cols = [
    "rate_lag_1",
    "rate_lag_3",
    "rate_lag_7",
    "rate_change",
    "rate_pct_change"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
print(f"🎯 Train: {train_df.count()} | Test: {test_df.count()}")

# =========================================================
# 5. Modèles
# =========================================================
lr = LinearRegression(
    featuresCol="features",
    labelCol="rate",
    predictionCol="predicted_rate_lr",
    maxIter=100,
    regParam=0.01
)

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="rate",
    predictionCol="predicted_rate_rf",
    numTrees=50,
    maxDepth=10,
    seed=42
)

pipeline_lr = Pipeline(stages=[assembler, scaler, lr])
pipeline_rf = Pipeline(stages=[assembler, scaler, rf])

print("🔧 Entraînement LR...")
model_lr = pipeline_lr.fit(train_df)

print("🔧 Entraînement RF...")
model_rf = pipeline_rf.fit(train_df)

# =========================================================
# 6. Évaluation
# =========================================================
pred_lr = model_lr.transform(test_df)
pred_rf = model_rf.transform(test_df)

evaluator = RegressionEvaluator(labelCol="rate", metricName="rmse")

rmse_lr = evaluator.evaluate(pred_lr, {evaluator.predictionCol: "predicted_rate_lr"})
rmse_rf = evaluator.evaluate(pred_rf, {evaluator.predictionCol: "predicted_rate_rf"})

print(f"📈 RMSE LR: {rmse_lr:.4f}")
print(f"📈 RMSE RF: {rmse_rf:.4f}")

best_predictions = pred_rf if rmse_rf < rmse_lr else pred_lr
prediction_col = "predicted_rate_rf" if rmse_rf < rmse_lr else "predicted_rate_lr"
model_name = "RandomForest" if rmse_rf < rmse_lr else "LinearRegression"

print(f"✅ Meilleur modèle: {model_name}")

# =========================================================
# 7. Export JSON POUR L’API (IMPORTANT)
# =========================================================
final_predictions = best_predictions.select(
    "target_currency",
    "rate_date",
    "rate",
    col(prediction_col).alias("predicted_rate")
)

# Global
final_predictions.coalesce(1).write \
    .mode("overwrite") \
    .json("/opt/spark/data/predictions")

# Par devise
for currency in top_currencies:
    final_predictions.filter(col("target_currency") == currency) \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .json(f"/opt/spark/data/predictions_{currency}")

print("💾 Prédictions exportées dans le volume partagé")

spark.stop()
print("🚀 Pipeline Spark terminé avec succès")
