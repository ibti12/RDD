# lab4_log_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, desc, count, min, max, avg, to_timestamp, hour

# -----------------------------
# 1. Création SparkSession
# -----------------------------
spark = SparkSession.builder \
    .appName("Day1-LogAnalysis") \
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

parsed_logs = raw_logs.select(
    regexp_extract('value', log_pattern, 1).alias('ip'),
    regexp_extract('value', log_pattern, 2).alias('timestamp'),
    regexp_extract('value', log_pattern, 3).alias('method'),
    regexp_extract('value', log_pattern, 4).alias('endpoint'),
    regexp_extract('value', log_pattern, 5).alias('protocol'),
    regexp_extract('value', log_pattern, 6).cast('int').alias('status'),
    regexp_extract('value', log_pattern, 7).cast('int').alias('response_time_ms')
)

parsed_logs.show(10, truncate=False)
parsed_logs.printSchema()

# -----------------------------
# 4. Vérification qualité des logs
# -----------------------------
total_logs = parsed_logs.count()
valid_logs = parsed_logs.filter(col("ip") != "").count()
invalid_logs = total_logs - valid_logs
print(f"Total logs: {total_logs}, Valid logs: {valid_logs}, Invalid logs: {invalid_logs}")

logs = parsed_logs.filter(col("ip") != "")

# -----------------------------
# 5. Statistiques et analyses
# -----------------------------
print("=== Status code distribution ===")
logs.groupBy("status").count().orderBy(desc("count")).show()

print("=== HTTP method distribution ===")
logs.groupBy("method").count().orderBy(desc("count")).show()

print("=== Top endpoints ===")
logs.groupBy("endpoint").count().orderBy(desc("count")).show()

print("=== Response time statistics ===")
response_time_stats = logs.agg(
    count("response_time_ms").alias("count"),
    min("response_time_ms").alias("min"),
    max("response_time_ms").alias("max"),
    avg("response_time_ms").alias("avg")
)
response_time_stats.show()

# -----------------------------
# 6. Trafic et timestamp
# -----------------------------
from pyspark.sql.functions import to_timestamp, hour, trim

# Nettoyer la colonne timestamp et convertir en timestamp
logs_with_time = logs.withColumn(
    "parsed_timestamp",
    to_timestamp(trim(col("timestamp")), "dd/MMM/yyyy:HH:mm:ss")
)

# Filtrer les logs invalides
logs_with_time = logs_with_time.filter(col("parsed_timestamp").isNotNull())

# Extraire l'heure
logs_with_time = logs_with_time.withColumn("hour", hour(col("parsed_timestamp")))




print("=== Traffic by hour ===")
logs_with_time.groupBy("hour").count().orderBy("hour").show()

# -----------------------------
# 7. User behaviour
# -----------------------------
print("=== Top 10 active IPs ===")
logs.groupBy("ip").count().orderBy(desc("count")).show(10)

# -----------------------------
# 8. Sauvegarde
# -----------------------------
logs_with_time.write.mode("overwrite").parquet("spark-data/ecommerce/processed_logs")

summary = logs.agg(
    count("*").alias("total_requests"),
    count("ip").alias("valid_requests"),
    count("endpoint").alias("unique_pages")
)

summary.show()
summary.coalesce(1).write.mode("overwrite").csv("spark-data/ecommerce/log_summary")

# -----------------------------
# 9. Stop Spark
# -----------------------------
spark.stop()
