# lab4_full_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, desc, count, min, max, avg, to_timestamp, hour, trim
)

# -----------------------------
# 1. Création SparkSession
# -----------------------------
spark = SparkSession.builder \
    .appName("Day1-FullLogAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# -----------------------------
# 2. Lecture des logs bruts
# -----------------------------
raw_logs = spark.read.text("spark-data/ecommerce/web_logs.txt")
print("Total log lines:", raw_logs.count())
raw_logs.show(3, truncate=False)

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

logs.show(10, truncate=False)
logs.printSchema()

# -----------------------------
# 4. Statistiques basiques
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
# 5. Conversion timestamp
# -----------------------------
logs_with_time = logs.withColumn(
    "parsed_timestamp",
    to_timestamp(trim(col("timestamp")), "dd/MMM/yyyy:HH:mm:ss")
).filter(col("parsed_timestamp").isNotNull()) \
 .withColumn("hour", hour(col("parsed_timestamp")))

print("=== Traffic by hour ===")
logs_with_time.groupBy("hour").count().orderBy("hour").show()

# -----------------------------
# 6. Top IPs
# -----------------------------
print("=== Top 10 active IPs ===")
logs.groupBy("ip").count().orderBy(desc("count")).show(10)

# -----------------------------
# 7. Sauvegarde des logs
# -----------------------------
logs_with_time.write.mode("overwrite").parquet("spark-data/ecommerce/processed_logs")
logs.coalesce(1).write.mode("overwrite").csv("spark-data/ecommerce/log_summary")

# -----------------------------
# 8. Challenges
# -----------------------------

# Challenge 1 — Bounce rate
total_visits = logs.groupBy("ip").count().count()
bounce_count = logs.groupBy("ip").agg(count("endpoint").alias("pages_visited")) \
    .filter(col("pages_visited") == 1).count()
bounce_rate = (bounce_count / total_visits) * 100 if total_visits > 0 else 0
print(f"Challenge 1 — Bounce Rate: {bounce_rate:.2f}%")

# Challenge 2 — Conversion funnel
steps = ["home", "products", "product", "cart", "checkout"]
print("Challenge 2 — Conversion Funnel (unique IPs per step):")
for step in steps:
    count_ips = logs.filter(col("endpoint").contains(step)).select("ip").distinct().count()
    print(f"{step.capitalize()}: {count_ips}")

# Challenge 3 — Peak traffic hour
peak_hour = logs_with_time.groupBy("hour").count().orderBy(desc("count")).first()["hour"]
print(f"Challenge 3 — Peak Traffic Hour: {peak_hour}:00")

# Challenge 4 — IPs with >50 requests/min or >50% 404
# Approximation (per hour)
ip_traffic = logs_with_time.groupBy("ip", "hour").count()
high_traffic_ips = ip_traffic.filter(col("count") > 50).select("ip").distinct().count()
if high_traffic_ips == 0:
    print("Challenge 4 — IPs with >50 requests/minute: Aucun IP détecté.")

ip_404 = logs.groupBy("ip").agg(
    (count(col("status") == 404).cast("int") / count("*")).alias("pct_404")
).filter(col("pct_404") > 0.5)
if ip_404.count() == 0:
    print("IPs with >50% 404 errors: Aucune IP avec plus de 50% de 404.")

# Challenge 5 — Slow endpoints
slow_endpoints = logs.groupBy("endpoint") \
    .agg(
        avg("response_time_ms").alias("avg_response_time"),
        count("*").alias("request_count")
    ).filter((col("avg_response_time") > 2000) & (col("request_count") > 10))
print("Challenge 5 — Slow endpoints (avg > 2000ms, >10 requests):")
slow_endpoints.orderBy(desc("avg_response_time")).show(truncate=False)

# Stop Spark
spark.stop()
