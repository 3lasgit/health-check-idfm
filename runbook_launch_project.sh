#!/bin/bash

# =============================================================================
# RUNBOOK : IDFM REAL-TIME HEALTH CHECK PIPELINE
# Description : Lancement de la pipeline Big Data (A à Z) 
# Infrastructure : Cluster 4 VMs (master, worker_0, worker_1, worker_2)
# =============================================================================

echo "--- DÉMARRAGE DE LA PIPELINE IDFM ---"

# -----------------------------------------------------------------------------
# ÉTAPE 1 : Services de Base (Hadoop HDFS & YARN) sur [master]
# -----------------------------------------------------------------------------
echo "[1/6] Lancement de HDFS et YARN (Storage & Cluster Management)[cite: 41, 42]..."
start-dfs.sh
start-yarn.sh

# -----------------------------------------------------------------------------
# ÉTAPE 2 : Messagerie (Kafka & Zookeeper) sur [master] et [worker_2]
# -----------------------------------------------------------------------------
echo "[2/6] Démarrage de Kafka et Zookeeper[cite: 6, 45]..."
# À exécuter sur master (et worker_2 pour le broker 2)
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
bin/kafka-server-start.sh -daemon config/server.properties

# -----------------------------------------------------------------------------
# ÉTAPE 3 : Configuration du Stockage et des Flux sur [master]
# -----------------------------------------------------------------------------
echo "[3/6] Configuration des topics Kafka et du Raw Landing HDFS[cite: 111, 112]..."
# Création des topics (3 partitions pour Spark, réplication 2 pour la tolérance) [cite: 109, 110]
bin/kafka-topics.sh --create --topic idfm-realtime --bootstrap-server master:9092,worker_2:9092 --partitions 3 --replication-factor 2
bin/kafka-topics.sh --create --topic idfm-processed --bootstrap-server master:9092,worker_2:9092 --partitions 3 --replication-factor 2

# Initialisation du dossier de stockage brut HDFS [cite: 7, 113]
hdfs dfs -mkdir -p /data/idfm/
hdfs dfs -touchz /data/idfm/raw_landing.json

# -----------------------------------------------------------------------------
# ÉTAPE 4 : Analytics Temps Réel (Spark Streaming) sur [master]
# -----------------------------------------------------------------------------
echo "[4/6] Lancement du traitement Spark Structured Streaming[cite: 8, 116, 117]..."
# Utilisation du package Kafka pour Spark SQL
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_analytics.py &

# -----------------------------------------------------------------------------
# ÉTAPE 5 : Persistance Time-Series (InfluxDB Bridge) sur [worker_0]
# -----------------------------------------------------------------------------
echo "[5/6] Connexion du Consumer Python vers InfluxDB[cite: 9, 118, 119]..."
# Note : Ce script tourne sur la machine dédiée à la visualisation (worker_0) [cite: 46]
# python3 influx_consumer.py &

# -----------------------------------------------------------------------------
# ÉTAPE 6 : Ingestion des données PRIM (Producer) sur [master]
# -----------------------------------------------------------------------------
echo "[6/6] Démarrage du Producer Python (API PRIM IDFM)[cite: 14, 105]..."
python3 producer.py

echo "--- PIPELINE OPÉRATIONNELLE ---"
echo "Accédez à Grafana sur http://worker_0:3000 pour la visualisation [cite: 10, 122]"