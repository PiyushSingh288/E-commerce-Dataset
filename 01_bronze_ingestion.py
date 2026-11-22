# ============================================================
# BRONZE LAYER — Raw CSV Ingestion into Delta Lake
# ============================================================

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.bronze")

spark.sql("SHOW VOLUMES IN workspace.default").show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

# ── Config ───────────────────────────────────────────────────
RAW_PATH   = "/Volumes/workspace/default/raw_data/Brazil E-Commerce/"

# ── Table registry ───────────────────────────────────────────
TABLES = {
    "orders"        : ("olist_orders_dataset.csv",               "order_id"),
    "order_items"   : ("olist_order_items_dataset.csv",          "order_id"),
    "payments"      : ("olist_order_payments_dataset.csv",       "order_id"),
    "reviews"       : ("olist_order_reviews_dataset.csv",        "review_id"),
    "customers"     : ("olist_customers_dataset.csv",            "customer_id"),
    "sellers"       : ("olist_sellers_dataset.csv",              "seller_id"),
    "products"      : ("olist_products_dataset.csv",             "product_id"),
    "geolocation"   : ("olist_geolocation_dataset.csv",          "geolocation_zip_code_prefix"),
    "category_names": ("product_category_name_translation.csv",  "product_category_name"),
}

# ── Ingestion function ───────────────────────────────────────
def ingest_to_bronze(table_name, csv_file, pk_col):
    print(f"\n>>> Ingesting: {table_name}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(f"{RAW_PATH}{csv_file}")
    )

    df = (
        df.withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", lit(csv_file))
    )

    row_count  = df.count()
    null_count = df.filter(df[pk_col].isNull()).count()
    dup_count  = row_count - df.dropDuplicates([pk_col]).count()

    print(f"    Rows       : {row_count:,}")
    print(f"    Null PKs   : {null_count}")
    print(f"    Duplicates : {dup_count}")

    # Write as managed Delta table — no path required
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"ecommerce_bronze.{table_name}")
        #Databricks handles storage location internally.
    )

    print(f"    Saved to   : ecommerce_bronze.{table_name}")
    return {"table": table_name, "rows": row_count, "null_pks": null_count, "duplicates": dup_count}

# ── Run all tables ───────────────────────────────────────────
results = []
for table_name, (csv_file, pk_col) in TABLES.items():
    result = ingest_to_bronze(table_name, csv_file, pk_col)
    results.append(result)

# ── Summary ──────────────────────────────────────────────────
print("\n" + "="*55)
print("BRONZE INGESTION SUMMARY")
print("="*55)
for r in results:
    status = "✅" if r["null_pks"] == 0 and r["duplicates"] == 0 else "⚠️"
    print(f"{status}  {r['table']:<20} | rows: {r['rows']:>7,} | null_pks: {r['null_pks']} | dups: {r['duplicates']}")
