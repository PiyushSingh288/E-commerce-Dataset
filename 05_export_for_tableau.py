import os

EXPORT_PATH = "/Volumes/workspace/default/raw_data/tableau_exports/"

# Create export folder
os.makedirs(EXPORT_PATH, exist_ok=True)

gold_tables = [
    "seller_performance",
    "monthly_revenue",
    "state_distribution",
    "late_delivery_heatmap",
    "category_performance",
]

for table in gold_tables:
    df = spark.table(f"ecommerce_gold.{table}")
    
    # Write as single CSV file
    (
        df.coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(f"{EXPORT_PATH}{table}_temp")
    )
    
    # Find the part file and rename it
    temp_path = f"{EXPORT_PATH}{table}_temp"
    files = [f.path for f in dbutils.fs.ls(temp_path) if f.name.startswith("part-")]
    
    if files:
        dbutils.fs.cp(files[0], f"{EXPORT_PATH}{table}.csv")
        dbutils.fs.rm(temp_path, recurse=True)
        print(f"✅ Exported: {table}.csv ({df.count():,} rows)")
    else:
        print(f"⚠️  Could not find part file for {table}")

print("\n✅ ALL EXPORTS COMPLETE")
print(f"📁 Files saved to: {EXPORT_PATH}")
