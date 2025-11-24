from pyspark.sql import SparkSession
from pyspark.sql.functions import when, round, lit, concat, lpad, format_string

spark = (
    SparkSession.builder
    .appName("Day1-TransformationPractice")
    .master("local[*]")
    .getOrCreate()
)

orders = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv("spark-data/ecommerce/orders.csv")
)

# -------------------------------------------------------
# Exercise 1 — FILTER
# -------------------------------------------------------
high_value = orders.filter(orders.totalAmount > 5000)
print("\nHigh-value orders count:", high_value.count())
high_value.show(5)

# -------------------------------------------------------
# Exercise 2 — SELECT & RENAME
# -------------------------------------------------------
order_summary = (
    orders.select(
        orders.orderNumber.alias("id"),
        orders.orderDate.alias("date"),
        orders.totalAmount.alias("amount"),
        "status",
    )
)

print("\n=== Order Summary (first 5) ===")
order_summary.show(5)

# -------------------------------------------------------
# Exercise 3 — ADD COLUMN
# -------------------------------------------------------
orders_categorized = (
    orders.withColumn(
        "orderSize",
        when(orders.totalAmount < 1000, "Small")
        .when(orders.totalAmount < 5000, "Medium")
        .otherwise("Large")
    )
)

orders_categorized.select("orderNumber", "totalAmount", "orderSize").show(10)

print("\n=== Distribution ===")
orders_categorized.groupBy("orderSize").count().orderBy("orderSize").show()

# -------------------------------------------------------
# Exercise 4 — CHAINING
# -------------------------------------------------------
processed_orders = (
    orders
    .filter(orders.status == "Shipped")
    .withColumn("amountRounded", round(orders.totalAmount, 0))
    .withColumn(
        "priority",
        when(orders.totalAmount > 5000, "High")
        .when(orders.totalAmount > 2000, "Medium")
        .otherwise("Low")
    )
    .select("orderNumber", "orderDate", "amountRounded", "priority", "status")
)

print("\nShipped orders count:", processed_orders.count())
processed_orders.show(10)

# -------------------------------------------------------
# Exercise 5 — DROP COLUMNS
# -------------------------------------------------------
original_columns = len(orders.columns)
reduced = orders.drop("requiredDate", "paymentMethod")
reduced_columns = len(reduced.columns)

print("\nOriginal columns:", original_columns)
print("Reduced columns:", reduced_columns)
reduced.show(5)

# -------------------------------------------------------
# Exercise 6 — DISTINCT
# -------------------------------------------------------
print("\n=== Distinct status ===")
orders.select("status").distinct().show()

print("\n=== Distinct paymentMethod ===")
orders.select("paymentMethod").distinct().show()

# ========================================================
# YOUR TURN — Additional tasks
# ========================================================

print("\n=== YOUR TURN TASKS ===")

# 1 — Filter from 2024-06-01
orders_after_june = orders.filter(orders.orderDate >= "2024-06-01")

# 2 — Boolean isLargeOrder
orders_marked = orders_after_june.withColumn(
    "isLargeOrder", orders_after_june.totalAmount > 3000
)

# 3 — Keep only Processing or Shipped
orders_proc_ship = orders_marked.filter(
    orders_marked.status.isin("Processing", "Shipped")
)

# 4 — orderCode = "ORDER-00001"
orders_final = orders_proc_ship.withColumn(
    "orderCode", format_string("ORDER-%05d", orders_proc_ship.orderNumber)
)

# 5 — Top 10 expensive
top10 = orders_final.orderBy(orders_final.totalAmount.desc()).limit(10)

top10.show(10, truncate=False)

spark.stop()
