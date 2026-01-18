from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import json

# Config InfluxDB 
token = ""
org = ""
bucket = "idfm_metrics"
client = InfluxDBClient(url="http://worker_2:8086", token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

consumer = KafkaConsumer(
    'idfm-processed',
    bootstrap_servers=['worker_2:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("En attente des données de Spark...")

for message in consumer:
    data = message.value
    
    # Création du point de donnée pour InfluxDB
    point = Point("health_check") \
        .tag("line", data['line']) \
        .tag("station", data['station']) \
        .field("delay_sec", float(data['delay_sec'])) \
        .field("status", data['status'])
    
    write_api.write(bucket=bucket, org=org, record=point)
    print(f"KPI stocké pour {data['line']} : {data['delay_sec']}s de décalage.")