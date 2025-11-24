from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder \
    .appName("Day1-LogAnalysis-CachingDemo") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Charger les logs
logs = spark.read.text("spark-data/ecommerce/web_logs.txt")

# --- 1. Sans cache ---
print("=== Sans cache ===")
start = time.time()
count1 = logs.count()
end = time.time()
print(f"First count(): {count1}, duration: {end-start:.2f}s")

start = time.time()
count2 = logs.count()
end = time.time()
print(f"Second count(): {count2}, duration: {end-start:.2f}s")

# --- 2. Avec cache ---
print("\n=== Avec cache ===")
logs_cached = logs.cache()

start = time.time()
count1_cached = logs_cached.count()
end = time.time()
print(f"First count() cached: {count1_cached}, duration: {end-start:.2f}s")

start = time.time()
count2_cached = logs_cached.count()
end = time.time()
print(f"Second count() cached: {count2_cached}, duration: {end-start:.2f}s")

speedup = (end-start) / ((end-start)/2 if (end-start) != 0 else 1)  # rough estimate
print(f"Speedup: {speedup:.2f}x (approx)")

input("Press Enter to exit Spark UI...")  # laisse le UI visible

spark.stop()
