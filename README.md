# Lambda Architecture for Exchange Rates Processing

## Description du projet

Ce projet consiste à mettre en place une **architecture Lambda complète** pour le traitement des **taux de change de l’euro (EUR)** vers d’autres devises, en combinant le **traitement temps réel** et le **traitement batch**.

L’objectif principal est de démontrer, de manière pratique, l’utilisation des technologies **Big Data open-source** pour :

* l’ingestion continue de données,
* le traitement temps réel à faible latence,
* le traitement batch fiable sur données historiques,
* l’automatisation des traitements,
* la visualisation et l’analyse des résultats.

Les données sont récupérées depuis l’API publique : [https://api.frankfurter.app](https://api.frankfurter.app)

<img width="474" height="264" alt="image" src="https://github.com/user-attachments/assets/53d55cdb-fec8-4907-9cda-8bc69165fe07" />

---

## Architecture Lambda

L’architecture Lambda repose sur quatre couches principales :

* **Ingestion (New Data)** : collecte continue des taux de change via **Kafka**.
* **Speed Layer** : traitement temps réel avec **Spark Structured Streaming**.
* **Batch Layer** : traitement périodique de l’historique avec **Spark Batch + Hive**.
* **Serving Layer** : interrogation et visualisation des données avec **Apache Zeppelin**.

L’ensemble de l’infrastructure est déployé et orchestré via **Docker Compose**.

<img width="1464" height="640" alt="image" src="https://github.com/user-attachments/assets/7eb3f9a8-d64e-4620-93e5-c851f6efc373" />

---

## Technologies utilisées

* **Apache Kafka** : ingestion des flux de données
* **Apache Spark 3.5** :

  * Structured Streaming (Speed Layer)
  * Batch Processing (Batch Layer)
* **Apache Hive** : stockage des données (Avro & Parquet)
* **Apache Airflow** : orchestration et automatisation
* **Apache Zeppelin** : visualisation et analyse
* **PostgreSQL** : metastore Hive
* **Docker & Docker Compose** : déploiement des services
* **Python** : scripts producer et jobs Spark

---

## Structure du projet

```
.
├── docker-compose.yml
├── hive-site.xml
├── producer.py
├── jobs/
│   ├── speed_layer.py
│   ├── batch_layer.py
│   └── verify_avro_parquet.py
├── dags/
│   └── batch_layer_pipeline.py
├── logs/
└── README.md
```

---

## Mise en place et exécution

### 1.Lancement de l’environnement

```bash
docker-compose up -d
```

Services accessibles :

* Spark Master : [http://localhost:8080](http://localhost:8080)
* Airflow : [http://localhost:8081](http://localhost:8081)
* Zeppelin : [http://localhost:8082](http://localhost:8082)
* MinIO : [http://localhost:9001](http://localhost:9001)

---

### 2.Lancement du Producer Kafka

```bash
python producer.py
```

Le producer envoie les taux de change toutes les **15 secondes** vers le topic Kafka `exchange-rates`.

---

### 3.Exécution de la Speed Layer

Traitement temps réel avec Spark Structured Streaming.

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
/opt/spark/jobs/speed_layer.py
```

---

### 4.Exécution de la Batch Layer

Traitement batch des données Kafka et stockage dans Hive.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
/opt/spark/jobs/batch_layer.py
```

Les données sont stockées sous forme de :

* **Master Data** : Avro (`exchange_rates_master`)
* **Batch View** : Parquet (`exchange_rates_daily_avg`)

---

### 5.Automatisation avec Apache Airflow

Un DAG Airflow exécute automatiquement :

* le Batch Layer,
* la vérification Avro / Parquet,
* la journalisation du succès.

DAG : `batch_layer_pipeline`

Planification : **toutes les heures** (`@hourly`).

---

### 6.Serving Layer et visualisation

Apache Zeppelin permet :

* la consultation des tables Hive,
* l’affichage des taux de change par devise et par date,
* le calcul des statistiques (min, max, moyenne).

---

## Résultats obtenus

* Traitement temps réel des taux de change
* Historisation fiable des données
* Agrégation journalière par devise
* Visualisation interactive via Zeppelin

---

## Contexte académique

**Mini-projet – Architectures et Infrastructures Big Data**
3ème année – Ingénierie Informatique et Technologies Émergentes
ENSA El Jadida – Université Chouaib Doukkali

---

## Réalisé par

* **BOUSENSAR Rajaa**
* **LAHLOU Asmae**
* **TAGHTI Zineb**

---

## Encadré par

* **Madame Lazar Hajar**

---

## Conclusion

Ce projet illustre concrètement la mise en œuvre d’une **architecture Lambda robuste**, combinant traitement temps réel et batch, et constitue une base solide pour des projets Big Data plus avancés (machine learning, systèmes de recommandation, détection d’anomalies, etc.).
