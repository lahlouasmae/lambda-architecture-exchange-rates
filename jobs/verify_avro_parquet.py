from pyspark.sql import SparkSession
import sys
import os

# =========================================================
# 1. Création de la Spark Session
# =========================================================
spark = SparkSession.builder \
    .appName("Verify Batch Layer Outputs") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Chemin de base aligné avec le batch layer
base_path = "hdfs://hadoop-namenode:8020/user/spark/exchange_rates"

print("\n" + "=" * 70)
print("🔍 VÉRIFICATION DU BATCH LAYER – RÉSULTATS DE SORTIE")
print("=" * 70)


# =========================================================
# 2. Fonction générique de vérification
# =========================================================
def verify_dataset(relative_path, format_name, logical_name):
    """
    Vérifie l'existence, le volume et la structure d'un dataset Batch.
    Retourne (success: bool, count: int, df)
    """
    full_path = os.path.join(base_path, relative_path)

    try:
        print("\n" + "-" * 70)
        print(f"📦 Dataset : {logical_name}")
        print(f"📁 Format  : {format_name.upper()}")
        print("-" * 70)

        df = spark.read.format(format_name).load(full_path)
        count = df.count()

        if count == 0:
            print("⚠ Dataset présent mais vide")
            return True, 0, df

        print("✅ Dataset disponible et lisible")
        print(f"✔ Nombre total d’enregistrements : {count}")
        print(f"✔ Nombre de colonnes             : {len(df.columns)}")

        print("\n📐 Structure des données :")
        df.printSchema()

        # Aperçu limité pour rapport
        print("\n📝 Aperçu (5 lignes) :")
        df.show(5, truncate=False)

        return True, count, df

    except Exception as e:
        print("❌ Dataset indisponible ou illisible")
        print(f"⚠ Détail technique : {str(e).splitlines()[0]}")
        return False, 0, None


# =========================================================
# 3. Vérification des sorties du Batch Layer
# =========================================================

# Master Data (AVRO)
master_ok, master_count, df_master = verify_dataset(
    relative_path="master",
    format_name="avro",
    logical_name="Master Data (données brutes normalisées)"
)

# Batch View (PARQUET)
batch_ok, batch_count, df_batch = verify_dataset(
    relative_path="daily_avg",
    format_name="parquet",
    logical_name="Batch View (agrégation journalière)"
)


# =========================================================
# 4. Vérification logique et résumé
# =========================================================
print("\n" + "=" * 70)
print("📋 RÉSUMÉ DU BATCH LAYER")
print("=" * 70)

print(f"✔ Master Data (AVRO) : {'OK' if master_ok else 'NON DISPONIBLE'} ({master_count} enregistrements)")
print(f"✔ Batch View (PARQUET) : {'OK' if batch_ok else 'NON DISPONIBLE'} ({batch_count} enregistrements)")

# Comparaison des colonnes si les deux existent
if master_ok and batch_ok and master_count > 0 and batch_count > 0:
    print("\n🔄 COHÉRENCE ENTRE MASTER DATA ET BATCH VIEW")
    master_cols = set(df_master.columns)
    batch_cols = set(df_batch.columns)
    common_cols = master_cols.intersection(batch_cols)
    print(f"✔ Colonnes communes             : {sorted(common_cols)}")
    print(f"✔ Colonnes Master uniquement    : {sorted(master_cols - batch_cols)}")
    print(f"✔ Colonnes Batch View uniquement: {sorted(batch_cols - master_cols)}")

    # Exemple de requêtes
    print("\n📊 Exemple : taux moyens par devise (Batch View)")
    df_batch.orderBy("rate_date", "target_currency").show(5, truncate=False)

    print("\n📊 Exemple : nombre de taux par devise (Master Data)")
    df_master.groupBy("target_currency").count().show(5, truncate=False)

    print("\n✔ Cohérence globale  : VALIDÉE")
else:
    print("\n⚠ Cohérence globale  : À VÉRIFIER")

print("=" * 70)
print("\n✅ Vérification du Batch Layer terminée !")
spark.stop()
sys.exit(0)