# ============================================================
# SILVER LAYER — Cleaned, Joined, Enriched Data
# ============================================================

from pyspark.sql.functions import (
    col, when, datediff, to_timestamp, avg, round,
    trim, upper, coalesce, lit, count
)
from pyspark.sql.functions import sum as spark_sum, first

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_silver")

# ── Load Bronze tables ───────────────────────────────────────
orders        = spark.table("ecommerce_bronze.orders")
order_items   = spark.table("ecommerce_bronze.order_items")
payments      = spark.table("ecommerce_bronze.payments")
reviews       = spark.table("ecommerce_bronze.reviews")
customers     = spark.table("ecommerce_bronze.customers")
sellers       = spark.table("ecommerce_bronze.sellers")
products      = spark.table("ecommerce_bronze.products")
geolocation   = spark.table("ecommerce_bronze.geolocation")
category_names= spark.table("ecommerce_bronze.category_names")

print("✅ Bronze tables loaded")

# ============================================================
# 1. ORDERS — parse timestamps, derive delivery delay
# ============================================================

orders_clean = (
    orders
    .withColumn("order_purchase_timestamp",  to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at",         to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
    # Delivery delay in days (negative = early, positive = late)
    .withColumn("delivery_delay_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date")
        )
    )
    # Flag late deliveries
    .withColumn("is_late_delivery",
        when(col("delivery_delay_days") > 0, 1).otherwise(0)
    )
    # Keep only meaningful order statuses
    .filter(col("order_status").isin("delivered", "shipped", "canceled", "processing"))
    .drop("_ingested_at", "_source_file")
)

print(f"✅ orders_clean: {orders_clean.count():,} rows")

# ============================================================
# 2. PAYMENTS — aggregate to one row per order
# ============================================================

payments_agg = (
    payments
    .groupBy("order_id")
    .agg(
        round(col("payment_value").cast("double"), 2).alias("payment_value"),
        count("payment_sequential").alias("payment_installments"),
        col("payment_type")
    )
    .drop("_ingested_at", "_source_file")
)

# For orders with multiple payment types, keep dominant one
payments_agg = (
    payments
    .groupBy("order_id", "payment_type")
    .agg(
        round(spark_sum(col("payment_value").cast("double")), 2).alias("payment_value"),
        count("payment_sequential").alias("payment_installments")
    )
    .dropDuplicates(["order_id"])
    .drop("_ingested_at", "_source_file")
)

print(f"✅ payments_agg: {payments_agg.count():,} rows")

# ============================================================
# 3. REVIEWS — deduplicate, keep latest per order
# ============================================================

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, to_timestamp

reviews_clean = (
    reviews
    .withColumn("review_creation_date", to_timestamp("review_creation_date"))
    .withColumn("rn", row_number().over(
        Window.partitionBy("order_id")
              .orderBy(col("review_creation_date").desc())
    ))
    .filter(col("rn") == 1)
    .drop("rn", "_ingested_at", "_source_file")
    .select("order_id", "review_score", "review_creation_date")
)

print(f"✅ reviews_clean: {reviews_clean.count():,} rows")

# ============================================================
# 4. PRODUCTS — join English category names
# ============================================================

products_clean = (
    products
    .join(category_names, on="product_category_name", how="left")
    .withColumn("category_en",
        coalesce(col("product_category_name_english"), lit("unknown"))
    )
    .select("product_id", "category_en", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm")
)

print(f"✅ products_clean: {products_clean.count():,} rows")

# ============================================================
# 5. GEOLOCATION — one lat/lng per zip code (avg)
# ============================================================

geo_clean = (
    geolocation
    .groupBy("geolocation_zip_code_prefix", "geolocation_state", "geolocation_city")
    .agg(
        round(avg("geolocation_lat"), 4).alias("lat"),
        round(avg("geolocation_lng"), 4).alias("lng")
    )
    .dropDuplicates(["geolocation_zip_code_prefix"])
)

print(f"✅ geo_clean: {geo_clean.count():,} rows")

# ============================================================
# 6. ORDER ITEMS — join products
# ============================================================

order_items_clean = (
    order_items
    .join(products_clean, on="product_id", how="left")
    .join(
        sellers.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
        on="seller_id", how="left"
    )
    .drop("_ingested_at", "_source_file")
)

print(f"✅ order_items_clean: {order_items_clean.count():,} rows")

# ============================================================
# 7. MASTER TABLE — join everything
# ============================================================

master = (
    orders_clean
    .join(customers.drop("_ingested_at", "_source_file"),
          on="customer_id", how="left")
    .join(payments_agg, on="order_id", how="left")
    .join(reviews_clean, on="order_id", how="left")
    .join(
        order_items_clean.groupBy("order_id", "seller_id", "category_en")
            .agg(round(avg("price"), 2).alias("avg_item_price")),
        on="order_id", how="left"
    )
)

print(f"✅ master: {master.count():,} rows")

# ============================================================
# 8. WRITE SILVER TABLES
# ============================================================

tables_to_write = {
    "orders"      : orders_clean,
    "payments"    : payments_agg,
    "reviews"     : reviews_clean,
    "products"    : products_clean,
    "geolocation" : geo_clean,
    "order_items" : order_items_clean,
    "master"      : master,
}

for name, df in tables_to_write.items():
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"ecommerce_silver.{name}")
    )
    print(f"✅ Saved: ecommerce_silver.{name}")

print("\n✅ SILVER LAYER COMPLETE")
