import requests
import json
import time
import os
from kafka import KafkaProducer

# Configuration
API_KEY = "8KaRC8hwXdU4PGagzFn7TtmaQIpHnkfg"
TOPIC = "idfm-realtime"
STATION_ID = "STIF:StopPoint:Q:41154:" # Croix de Berny
URL = f"https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring?MonitoringRef={STATION_ID}"

producer = KafkaProducer(
    bootstrap_servers=["master:9092", "worker_2:9092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def run_pipeline():
    headers = {"apikey": API_KEY}
    try:
        response = requests.get(URL, headers=headers).json()
        deliveries = response['Siri']['ServiceDelivery']['StopMonitoringDelivery']
        
        for delivery in deliveries:
            visits = delivery.get('MonitoredStopVisit', [])
            for visit in visits:
                journey = visit['MonitoredVehicleJourney']
                call = journey['MonitoredCall']
                
                # Schéma canonique pour Spark [cite: 100]
                data = {
                    "line": journey['LineRef']['value'],
                    "station": call['StopPointName'][0]['value'],
                    "theoretical": call.get('AimedArrivalTime'),
                    "predicted": call.get('ExpectedArrivalTime'),
                    "status": call.get('DepartureStatus'),
                    "timestamp": visit['RecordedAtTime']
                }

                # 1. Envoi Kafka [cite: 105]
                producer.send(TOPIC, key=data['line'].encode('utf-8'), value=data)

                # 2. Stockage Brut HDFS (Landing) [cite: 112, 113]
                # Commande simplifiée pour le TP : on append dans un fichier JSON sur HDFS
                line_json = json.dumps(data).replace("'", '"')
                os.system(f"echo '{line_json}' | hdfs dfs -appendToFile - /data/idfm/raw_landing.json")

        print(f"Update effectuée : {len(visits)} trains traités.")
    except Exception as e:
        print(f"Erreur : {e}")

while True:
    run_pipeline()
    time.sleep(60) # Polling toutes les minutes