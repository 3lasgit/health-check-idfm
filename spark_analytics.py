from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# 1. Définition du schéma (doit correspondre à ton Producer Python)
schema = StructType([
    StructField("line", StringType()),
    StructField("station", StringType()),
    StructField("theoretical", StringType()),
    StructField("predicted", StringType()),
    StructField("timestamp", StringType())
])

# 2. Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("IDFM-Spark-Analytics") \
    .getOrCreate()

# 3. Lecture du flux depuis Kafka [cite: 6]
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "master:9092", "worker_2:9092") \
    .option("subscribe", "idfm-realtime") \
    .load()

# 4. Désérialisation du JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Calcul du Retard (Health Check)
# On convertit les ISO8601 en timestamps Unix pour faire la soustraction
# Retard = (Prédit - Théorique). Un résultat positif = retard, négatif = avance.
analytics_df = json_df.withColumn(
    "delay_sec", 
    unix_timestamp(col("predicted")) - unix_timestamp(col("theoretical"))
)

# 6. Écriture des résultats
query = analytics_df.selectExpr("CAST(line AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "master:9092", "worker_2:9092") \
    .option("topic", "idfm-processed") \
    .option("checkpointLocation", "/tmp/checkpoints_spark") \
    .start()