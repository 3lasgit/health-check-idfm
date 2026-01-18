from kafka import KafkaProducer
import requests, json
import time

producer = KafkaProducer(bootstrap_servers=["master:9092", "worker_2:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),)    

t_max = time.time() + 3600

while time.time() < t_max : 
    metrics = requests.get("http://master:8088/ws/v1/cluster/metrics",
                           auth=('admin', 'admin')).json()
    producer.send("hadoop-metrics", metrics)
    producer.flush()
    time.sleep(20)
