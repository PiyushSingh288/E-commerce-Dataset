from pyspark.sql.functions import (
    col, round, avg, sum as spark_sum, count,
    countDistinct, when, month, year, concat_ws
)

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_gold")

# ── Load Silver tables ───────────────────────────────────────
master      = spark.table("ecommerce_silver.master")
order_items = spark.table("ecommerce_silver.order_items")
orders      = spark.table("ecommerce_silver.orders")
reviews     = spark.table("ecommerce_silver.reviews")

print("✅ Silver tables loaded")

seller_performance = (
    order_items
    .join(orders.select("order_id", "order_status", "is_late_delivery"),
          on="order_id", how="left")
    .join(reviews.select("order_id", "review_score"),
          on="order_id", how="left")
    .groupBy("seller_id", "seller_city", "seller_state")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(spark_sum(col("price").cast("double")), 2).alias("total_revenue"),
        round(avg(col("price").cast("double")), 2).alias("avg_order_value"),
        round(avg("review_score"), 2).alias("avg_review_score"),
        round(
            (spark_sum("is_late_delivery") / count("order_id")) * 100, 2
        ).alias("late_delivery_pct")
    )
    .filter(col("total_orders") >= 5)
    .orderBy(col("total_revenue").desc())
)

seller_performance.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce_gold.seller_performance")

print(f"✅ seller_performance: {seller_performance.count():,} rows")

monthly_revenue = (
    order_items
    .join(orders.select("order_id", "order_purchase_timestamp", "order_status"),
          on="order_id", how="left")
    .filter(col("order_status") == "delivered")
    .withColumn("year",  year("order_purchase_timestamp"))
    .withColumn("month", month("order_purchase_timestamp"))
    .withColumn("year_month", concat_ws("-", col("year"), col("month")))
    .groupBy("year_month", "year", "month", "category_en")
    .agg(
        round(spark_sum(col("price").cast("double")), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        round(avg(col("price").cast("double")), 2).alias("avg_order_value")
    )
    .orderBy("year", "month")
)

monthly_revenue.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce_gold.monthly_revenue")

print(f"✅ monthly_revenue: {monthly_revenue.count():,} rows")

state_distribution = (
    master
    .filter(col("order_status") == "delivered")
    .groupBy("customer_state")
    .agg(
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("order_id").alias("total_orders"),
        round(spark_sum(col("payment_value").cast("double")), 2).alias("total_revenue"),
        round(avg(col("payment_value").cast("double")), 2).alias("avg_order_value"),
        round(avg("review_score"), 2).alias("avg_review_score")
    )
    .orderBy(col("total_revenue").desc())
)

state_distribution.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce_gold.state_distribution")

print(f"✅ state_distribution: {state_distribution.count():,} rows")

late_delivery_heatmap = (
    order_items
    .join(orders.select("order_id", "is_late_delivery", "delivery_delay_days"),
          on="order_id", how="left")
    .groupBy("seller_city", "seller_state")
    .agg(
        count("order_id").alias("total_orders"),
        spark_sum("is_late_delivery").alias("late_orders"),
        round(
            (spark_sum("is_late_delivery") / count("order_id")) * 100, 2
        ).alias("late_delivery_pct"),
        round(avg("delivery_delay_days"), 1).alias("avg_delay_days")
    )
    .filter(col("total_orders") >= 10)
    .orderBy(col("late_delivery_pct").desc())
)

late_delivery_heatmap.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce_gold.late_delivery_heatmap")

print(f"✅ late_delivery_heatmap: {late_delivery_heatmap.count():,} rows")

category_performance = (
    order_items
    .join(orders.select("order_id", "order_status"), on="order_id", how="left")
    .join(reviews.select("order_id", "review_score"), on="order_id", how="left")
    .filter(col("order_status") == "delivered")
    .groupBy("category_en")
    .agg(
        round(spark_sum(col("price").cast("double")), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        round(avg(col("price").cast("double")), 2).alias("avg_price"),
        round(avg("review_score"), 2).alias("avg_review_score")
    )
    .orderBy(col("total_revenue").desc())
)

category_performance.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce_gold.category_performance")

print(f"✅ category_performance: {category_performance.count():,} rows")

print("\n✅ GOLD LAYER COMPLETE")

spark.table("ecommerce_gold.vw_category_performance").printSchema()

spark.table("ecommerce_gold.category_performance").printSchema()

df = spark.table("ecommerce_gold.vw_category_performance")

(
    df.coalesce(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv("/Volumes/workspace/default/raw_data/tableau_exports/category_performance_v2_temp")
)

files = [f.path for f in dbutils.fs.ls("/Volumes/workspace/default/raw_data/tableau_exports/category_performance_v2_temp") if f.name.startswith("part-")]
dbutils.fs.cp(files[0], "/Volumes/workspace/default/raw_data/tableau_exports/category_performance_v2.csv")
dbutils.fs.rm("/Volumes/workspace/default/raw_data/tableau_exports/category_performance_v2_temp", recurse=True)
print("✅ Done")

df = spark.table("ecommerce_gold.vw_seller_performance")

(
    df.coalesce(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv("/Volumes/workspace/default/raw_data/tableau_exports/seller_performance_v2_temp")
)

files = [f.path for f in dbutils.fs.ls("/Volumes/workspace/default/raw_data/tableau_exports/seller_performance_v2_temp") if f.name.startswith("part-")]
dbutils.fs.cp(files[0], "/Volumes/workspace/default/raw_data/tableau_exports/seller_performance_v2.csv")
dbutils.fs.rm("/Volumes/workspace/default/raw_data/tableau_exports/seller_performance_v2_temp", recurse=True)
print("✅ Done")

df = spark.table("ecommerce_gold.vw_seller_performance")

(
    df.coalesce(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv("/Volumes/workspace/default/raw_data/tableau_exports/seller_performance_v2_temp")
)

files = [f.path for f in dbutils.fs.ls("/Volumes/workspace/default/raw_data/tableau_exports/seller_performance_v2_temp") if f.name.startswith("part-")]
dbutils.fs.cp(files[0], "/Volumes/workspace/default/raw_data/tableau_exports/seller_performance_v2.csv")
dbutils.fs.rm("/Volumes/workspace/default/raw_data/tableau_exports/seller_performance_v2_temp", recurse=True)
print("✅ Done")

# Export state_distribution and late_delivery_heatmap views
exports = {
    "state_distribution_v2": "ecommerce_gold.vw_state_distribution",
    "late_delivery_v2": "ecommerce_gold.vw_late_delivery_heatmap",
}

base = "/Volumes/workspace/default/raw_data/tableau_exports/"

for filename, table in exports.items():
    df = spark.table(table)
    temp = f"{base}{filename}_temp"
    (
        df.coalesce(1)
        .write.option("header", "true")
        .mode("overwrite")
        .csv(temp)
    )
    files = [f.path for f in dbutils.fs.ls(temp) if f.name.startswith("part-")]
    dbutils.fs.cp(files[0], f"{base}{filename}.csv")
    dbutils.fs.rm(temp, recurse=True)
    print(f"✅ {filename}.csv exported")
