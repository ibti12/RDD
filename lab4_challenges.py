from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, when, avg, window, desc

# -------------------------------
# Initialisation Spark
# -------------------------------
spark = SparkSession.builder \
    .appName("Lab4-Challenges") \
    .master("local[*]") \
    .getOrCreate()

# Charger les logs traités
try:
    logs_with_time = spark.read.parquet("spark-data/ecommerce/processed_logs")
except Exception as e:
    print("Erreur: impossible de charger processed_logs. Vérifie que lab4_log_analysis.py a été exécuté correctement.")
    print(e)
    spark.stop()
    exit(1)

if logs_with_time.count() == 0:
    print("Le DataFrame logs_with_time est vide. Vérifie que les logs traités existent.")
    spark.stop()
    exit(1)

# -------------------------------
# Challenge 1 — Bounce Rate
# -------------------------------
requests_per_ip = logs_with_time.groupBy("ip").count()
bounced_ips = requests_per_ip.filter(col("count") == 1).count()
total_ips = requests_per_ip.count()

if total_ips > 0:
    bounce_rate = bounced_ips / total_ips * 100
    print(f"\nChallenge 1 — Bounce Rate: {bounce_rate:.2f}%")
else:
    print("\nChallenge 1 — Bounce Rate: Aucun IP trouvé.")

# -------------------------------
# Challenge 2 — Conversion Funnel
# -------------------------------
home_ips = logs_with_time.filter(col("endpoint") == "/home").select("ip").distinct()
products_ips = logs_with_time.filter(col("endpoint") == "/products").select("ip").distinct()
product_detail_ips = logs_with_time.filter(col("endpoint").startswith("/product/")).select("ip").distinct()
cart_ips = logs_with_time.filter(col("endpoint") == "/cart").select("ip").distinct()
checkout_ips = logs_with_time.filter(col("endpoint") == "/checkout").select("ip").distinct()

print("\nChallenge 2 — Conversion Funnel (unique IPs per step):")
print("Home:", home_ips.count())
print("Products:", products_ips.count())
print("Product Detail:", product_detail_ips.count())
print("Cart:", cart_ips.count())
print("Checkout:", checkout_ips.count())

# -------------------------------
# Challenge 3 — Peak Traffic Hour
# -------------------------------
traffic_by_hour = logs_with_time.withColumn("hour", hour(col("parsed_timestamp"))) \
    .groupBy("hour").count().orderBy("hour")
traffic_by_hour.show()

if traffic_by_hour.count() > 0:
    peak_hour = traffic_by_hour.orderBy(col("count").desc()).first()["hour"]
    print(f"\nChallenge 3 — Peak Traffic Hour: {peak_hour}:00")
else:
    print("\nChallenge 3 — Peak Traffic Hour: aucun trafic détecté.")

# -------------------------------
# Challenge 4 — Suspicious Activity
# -------------------------------
# IPs avec >50 requêtes par minute
requests_per_minute = logs_with_time.groupBy(
    "ip", window(col("parsed_timestamp"), "1 minute")
).count()

suspicious_ips = requests_per_minute.filter(col("count") > 50)
print("\nChallenge 4 — IPs with >50 requests/minute:")
if suspicious_ips.count() > 0:
    suspicious_ips.show(10, truncate=False)
else:
    print("Aucune IP détectée avec plus de 50 requêtes/minute.")

# IPs avec >50% 404
ip_404_ratio = logs_with_time.groupBy("ip").agg(
    (count(when(col("status") == 404, True)) / count("*")).alias("404_ratio"),
    count("*").alias("total_requests")
)
ip_high_404 = ip_404_ratio.filter(col("404_ratio") > 0.5)
print("IPs with >50% 404 errors:")
if ip_high_404.count() > 0:
    ip_high_404.show(10, truncate=False)
else:
    print("Aucune IP avec plus de 50% de 404.")

# -------------------------------
# Challenge 5 — Performance Bottlenecks
# -------------------------------
slow_endpoints = logs_with_time.groupBy("endpoint").agg(
    avg("response_time_ms").alias("avg_response_time"),
    count("*").alias("request_count")
).filter(col("avg_response_time") > 2000).filter(col("request_count") > 10)

print("\nChallenge 5 — Slow endpoints (avg > 2000ms, >10 requests):")
if slow_endpoints.count() > 0:
    slow_endpoints.show(truncate=False)
else:
    print("Aucun endpoint lent détecté.")

slow_status = logs_with_time.groupBy("endpoint", "status").agg(
    avg("response_time_ms").alias("avg_response_time")
).filter(col("avg_response_time") > 2000).orderBy(desc("avg_response_time"))

print("Slow endpoints by status:")
if slow_status.count() > 0:
    slow_status.show(truncate=False)
else:
    print("Aucun endpoint lent par status détecté.")

# -------------------------------
# Fin Spark
# -------------------------------
spark.stop()
