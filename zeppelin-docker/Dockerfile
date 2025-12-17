# ====================================================
# Stage 1: Téléchargement des dépendances (mise en cache)
# ====================================================
FROM alpine:3.18 AS downloader

WORKDIR /tmp

# Téléchargement de Spark (sera mis en cache si l'URL ne change pas)
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz && \
    rm spark-3.5.0-bin-hadoop3.tgz

# Téléchargement des JARs en parallèle (sera aussi mis en cache)
RUN mkdir -p /jars && cd /jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar & \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar & \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar & \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar & \
    wait

# ====================================================
# Stage 2: Image finale Zeppelin
# ====================================================
FROM apache/zeppelin:0.11.2

USER root

# Copie des fichiers depuis le stage downloader (TRÈS RAPIDE)
COPY --from=downloader /tmp/spark-3.5.0-bin-hadoop3 /opt/spark-3.5.0-bin-hadoop3
COPY --from=downloader /jars /opt/spark-3.5.0-bin-hadoop3/extra-jars

# Configuration rapide (quelques secondes seulement)
RUN ln -s /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    mkdir -p /opt/spark/conf && \
    chmod -R 755 /opt/spark

# Variables d'environnement
ENV SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin:/opt/spark/sbin \
    SPARK_CLASSPATH=/opt/spark/extra-jars/*

# On reste en root pour éviter les problèmes de permissions
WORKDIR /opt/zeppelin