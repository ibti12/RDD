# lab4_full_analysis_visual.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, desc, count, min, max, avg, to_timestamp, hour, trim
)
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# -----------------------------
# 1. Création SparkSession
# -----------------------------
spark = SparkSession.builder \
    .appName("Day1-FullLogAnalysis-Visual") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# -----------------------------
# 2. Lecture des logs bruts
# -----------------------------
raw_logs = spark.read.text("spark-data/ecommerce/web_logs.txt")
print("Total log lines:", raw_logs.count())

# -----------------------------
# 3. Parsing des logs avec regex
# -----------------------------
log_pattern = r'(\S+) - - \[([\w:/]+\s*)\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+) ".*"'

logs = raw_logs.select(
    regexp_extract('value', log_pattern, 1).alias('ip'),
    regexp_extract('value', log_pattern, 2).alias('timestamp'),
    regexp_extract('value', log_pattern, 3).alias('method'),
    regexp_extract('value', log_pattern, 4).alias('endpoint'),
    regexp_extract('value', log_pattern, 5).alias('protocol'),
    regexp_extract('value', log_pattern, 6).cast('int').alias('status'),
    regexp_extract('value', log_pattern, 7).cast('int').alias('response_time_ms')
).filter(col("ip") != "")

# -----------------------------
# 4. Conversion timestamp et hour
# -----------------------------
logs_with_time = logs.withColumn(
    "parsed_timestamp",
    to_timestamp(trim(col("timestamp")), "dd/MMM/yyyy:HH:mm:ss")
).filter(col("parsed_timestamp").isNotNull()) \
 .withColumn("hour", hour(col("parsed_timestamp")))

# -----------------------------
# 5. Statistiques de base
# -----------------------------
print("=== Status code distribution ===")
logs.groupBy("status").count().orderBy(desc("count")).show()

print("=== HTTP method distribution ===")
logs.groupBy("method").count().orderBy(desc("count")).show()

print("=== Top endpoints ===")
logs.groupBy("endpoint").count().orderBy(desc("count")).show()

print("=== Response time statistics ===")
logs.agg(
    count("response_time_ms").alias("count"),
    min("response_time_ms").alias("min"),
    max("response_time_ms").alias("max"),
    avg("response_time_ms").alias("avg")
).show()

# -----------------------------
# 6. Visualisation - Traffic par heure
# -----------------------------
hourly_traffic = logs_with_time.groupBy("hour").count().orderBy("hour").toPandas()
plt.figure(figsize=(10,5))
sns.barplot(data=hourly_traffic, x="hour", y="count", palette="Blues_d")
plt.title("Trafic par heure")
plt.xlabel("Heure")
plt.ylabel("Nombre de requêtes")
plt.tight_layout()
plt.savefig("hourly_traffic.png")
plt.show()

# -----------------------------
# 7. Visualisation - Funnel de conversion
# -----------------------------
steps = ["home", "products", "product", "cart", "checkout"]
funnel_data = []
for step in steps:
    count_ips = logs.filter(col("endpoint").contains(step)).select("ip").distinct().count()
    funnel_data.append(count_ips)

plt.figure(figsize=(8,5))
sns.barplot(x=steps, y=funnel_data, palette="viridis")
plt.title("Funnel de conversion")
plt.xlabel("Étape")
plt.ylabel("Nombre d'IPs uniques")
plt.tight_layout()
plt.savefig("conversion_funnel.png")
plt.show()

# -----------------------------
# 8. Visualisation - Endpoints lents
# -----------------------------
slow_endpoints = logs.groupBy("endpoint") \
    .agg(
        avg("response_time_ms").alias("avg_response_time"),
        count("*").alias("request_count")
    ).filter((col("avg_response_time") > 2000) & (col("request_count") > 10)) \
    .orderBy(desc("avg_response_time"))

slow_df = slow_endpoints.toPandas()
plt.figure(figsize=(10,5))
sns.barplot(data=slow_df, y="endpoint", x="avg_response_time", palette="Reds_r")
plt.title("Endpoints lents (avg > 2000ms, >10 requêtes)")
plt.xlabel("Temps de réponse moyen (ms)")
plt.ylabel("Endpoint")
plt.tight_layout()
plt.savefig("slow_endpoints.png")
plt.show()

# -----------------------------
# 9. Challenges
# -----------------------------
# Challenge 1 — Bounce rate
total_visits = logs.groupBy("ip").count().count()
bounce_count = logs.groupBy("ip").agg(count("endpoint").alias("pages_visited")) \
    .filter(col("pages_visited") == 1).count()
bounce_rate = (bounce_count / total_visits) * 100 if total_visits > 0 else 0
print(f"Challenge 1 — Bounce Rate: {bounce_rate:.2f}%")

# Challenge 2 — Conversion funnel (déjà visualisé)
print("Challenge 2 — Conversion Funnel (unique IPs per step):")
for step, count_ips in zip(steps, funnel_data):
    print(f"{step.capitalize()}: {count_ips}")

# Challenge 3 — Peak traffic hour
peak_hour = logs_with_time.groupBy("hour").count().orderBy(desc("count")).first()["hour"]
print(f"Challenge 3 — Peak Traffic Hour: {peak_hour}:00")

# Challenge 4 — IPs avec >50 req/min ou >50% 404 (approx)
ip_traffic = logs_with_time.groupBy("ip", "hour").count()
high_traffic_ips = ip_traffic.filter(col("count") > 50).select("ip").distinct().count()
if high_traffic_ips == 0:
    print("Challenge 4 — IPs avec >50 requêtes/minute: Aucun IP détecté.")

ip_404 = logs.groupBy("ip").agg(
    (count(col("status") == 404).cast("int") / count("*")).alias("pct_404")
).filter(col("pct_404") > 0.5)
if ip_404.count() == 0:
    print("IPs avec >50% de 404: Aucun IP détecté.")

# Challenge 5 — Endpoints lents (déjà visualisé)
print("Challenge 5 — Endpoints lents affichés en graphique.")

# -----------------------------
# 10. Sauvegarde
# -----------------------------
logs_with_time.write.mode("overwrite").parquet("spark-data/ecommerce/processed_logs")
logs.coalesce(1).write.mode("overwrite").csv("spark-data/ecommerce/log_summary")

# Stop Spark
spark.stop()
