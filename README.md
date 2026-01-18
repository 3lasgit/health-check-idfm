# IDFM Real-Time Health Check Pipeline

Ce projet implémente une pipeline Big Data de bout en bout pour surveiller la santé du réseau RER B (Station : La Croix de Berny) en comparant les horaires théoriques et réels en temps réel.

## 1. Infrastructure du Cluster (Mapping)

Le cluster est déployé sur 4 VMs Azure :

| Machine | Composants installés | Rôle |
|---------|---------------------|------|
| master | NameNode, ResourceManager, Kafka Broker, Zookeeper, Spark Master | Gestionnaire du Cluster |
| worker_1 | DataNode, NodeManager, Spark Executor | Stockage & Calcul |
| worker_2 | DataNode, NodeManager, Kafka Broker, Spark Executor, InfluxDB, Grafana, Python Influx-Consumer | Stockage, Messaging, Calcul & Visualisation |
| worker_0 | DataNode, NodeManager | Stockage & Calcul |

## 2. Pipeline de Données

### Flux de traitement

1. **Ingestion** : `producer.py` interroge l'API PRIM, envoie au topic Kafka `idfm-realtime` et sauvegarde le brut dans HDFS (`/data/idfm/raw_landing.json`)

2. **Traitement** : `spark_analytics.py` calcule le `delay_sec` (Prédit - Théorique) et renvoie le résultat vers Kafka (`idfm-processed`)

3. **Stockage Time-Series** : `influx_consumer.py` consomme les résultats et les écrit dans InfluxDB

4. **Dashboard** : Grafana affiche la santé de la ligne via des jauges et des séries temporelles

## 3. Runbook (Ordre d'exécution)

Suivez cet ordre scrupuleusement pour garantir la cohérence des flux :

### Étape 1 : Démarrage des Services de Base (sur `master`)

```bash
start-dfs.sh
start-yarn.sh
# Démarrer Zookeeper et Kafka sur master et worker_2
bin/kafka-server-start.sh -daemon config/server.properties
```

### Étape 2 : Création des Topics Kafka

```bash
bin/kafka-topics.sh --create --topic idfm-realtime --bootstrap-server master:9092 --partitions 3 --replication-factor 2
bin/kafka-topics.sh --create --topic idfm-processed --bootstrap-server master:9092 --partitions 3 --replication-factor 2
```

### Étape 3 : Lancement de l'Analytics (sur `master`)

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_analytics.py
```

### Étape 4 : Lancement de l'Ingestion InfluxDB (sur `worker_2`)

(Utilise ce script pour remplacer ton doublon)

```python
# influx_consumer.py sur worker_2
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json

client = InfluxDBClient(url="http://localhost:8086", token="TON_TOKEN", org="TON_ORG")
write_api = client.write_api(write_options=SYNCHRONOUS)
consumer = KafkaConsumer('idfm-processed', bootstrap_servers=['master:9092', 'worker_2:9092'],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for msg in consumer:
    p = Point("health").tag("line", msg.value['line']).field("delay", float(msg.value['delay_sec']))
    write_api.write(bucket="idfm_metrics", record=p)
```

### Étape 5 : Lancement du Producer (sur `master`)

```bash
python3 producer.py
```

## Architecture

```
API PRIM → Producer (Kafka) → Spark Analytics → Kafka → InfluxDB → Grafana
                ↓
             HDFS (raw_landing.json)
```

## Prérequis

- Hadoop HDFS
- Apache Kafka
- Apache Spark 3.5.0
- InfluxDB
- Grafana
- Python 3.x avec les packages : kafka-python, influxdb-client
