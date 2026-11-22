# Brazilian E-Commerce Analytics Pipeline

An end-to-end data engineering project for the Olist Brazilian E-Commerce dataset. The pipeline uses Databricks, PySpark, Delta Lake, and a Bronze → Silver → Gold medallion architecture to turn nine raw CSV files into analytics tables and Tableau-ready CSV exports.

## Project status

The pipeline was run in Databricks using **Serverless compute**. The run completed the Bronze ingestion, Silver transformations, five Gold table writes, creation of five SQL views, and export of five CSV files.

| Stage | Reported status |
|---|---|
| Bronze ingestion | Completed; all nine source tables loaded |
| Silver transformations | Completed; seven Silver tables saved |
| Gold aggregations | Completed; five Gold tables saved |
| SQL views | Completed; five views created over the Gold tables |
| CSV exports | Completed; five CSVs reported at the export path |
| Tableau dashboard | An existing Tableau Public link is listed below; a new dashboard was not created or verified as part of this run |
| Export downloads | The CSVs were not downloaded to the local machine |
| Workspace cleanup | Moving the extra `New File 2026-11-21 12:55:52.py` to Trash was attempted, but the Databricks UI did not confirm completion; check the project folder manually |

The workspace files were edited to use the uploaded dataset location and to address execution issues encountered during the run. The local `.py` files in this repository may not exactly match the versions executed in the Databricks Workspace; synchronize them before treating this repository as a fully reproducible copy of the workspace pipeline.

## Architecture

```text
Nine Olist CSV files
        │
        ▼
Bronze Delta tables ── raw ingested data and audit columns
        │
        ▼
Silver Delta tables ─── cleaned, deduplicated and enriched data
        │
        ▼
Gold Delta tables ───── business-oriented aggregates
        │
        ├── Databricks SQL views
        └── CSV exports for Tableau
```

![Pipeline architecture](screenshots/architecture.png)

## Pipeline files

Run the Databricks Workspace files in numerical order:

1. **`01_bronze_ingestion.py` — Ingest raw CSVs.** Reads the nine source files, adds ingestion metadata, reports row/null/duplicate counts, and writes Delta tables to `ecommerce_bronze`.
2. **`02_silver_cleaning.py` — Clean and enrich data.** Parses order and review timestamps, derives delivery-delay fields, aggregates payments, deduplicates reviews, enriches products with English category names, prepares geolocation and order-item data, and writes seven tables to `ecommerce_silver`.
3. **`03_gold_aggregations.py` — Build business summaries.** Produces `seller_performance`, `monthly_revenue`, `state_distribution`, `late_delivery_heatmap`, and `category_performance` in `ecommerce_gold`.
4. **`04_databricks_sql_views.py` — Add SQL views.** Creates five views over the Gold tables for querying and downstream analytics. In the successful run described here, these views were simple `SELECT *` views; additional business logic described in earlier documentation was not part of that run.
5. **`05_export_for_tableau.py` — Export CSVs.** Writes one CSV for each Gold table to the Tableau export directory.

### Gold tables

| Table | Contents in the executed pipeline |
|---|---|
| `ecommerce_gold.seller_performance` | Order count, revenue, average item price, and average freight by seller and seller state |
| `ecommerce_gold.monthly_revenue` | Revenue and order count by purchase year and month |
| `ecommerce_gold.state_distribution` | Revenue and order count by customer state |
| `ecommerce_gold.late_delivery_heatmap` | Average delivery delay, late orders, and total orders by customer state and purchase month |
| `ecommerce_gold.category_performance` | Revenue, order count, and average item price by English product category |

The exact metrics available to a dashboard depend on the schema of the currently exported CSVs. Check the CSV headers before building visuals, and avoid summing measures across tables with different levels of detail.

## Databricks locations

The successful run used Serverless compute and these Unity Catalog Volume paths:

| Purpose | Path |
|---|---|
| Uploaded input CSV files | `/Volumes/workspace/default/raw_data/dataset/` |
| Tableau CSV exports | `/Volumes/workspace/default/raw_data/tableau_exports/` |

The Bronze tables are in `ecommerce_bronze`, Silver tables in `ecommerce_silver`, and Gold tables and views in `ecommerce_gold`.

## Dataset

- **Source:** [Olist Brazilian E-Commerce dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Coverage:** 2016–2018
- **Files:** nine CSVs covering orders, items, payments, reviews, customers, sellers, products, geolocation, and product category translations.

The successful Bronze run reported these row counts:

| Bronze table | Rows |
|---|---:|
| `orders` | 99,441 |
| `order_items` | 112,650 |
| `payments` | 103,886 |
| `reviews` | 99,249 |
| `customers` | 99,441 |
| `sellers` | 3,095 |
| `products` | 32,951 |
| `geolocation` | 1,000,163 |
| `category_names` | 71 |

These are counts reported during that run, not automatically refreshed results. They provide useful context: items and payments are at a finer grain than orders, and not every order has a review.

## Running the pipeline

1. Upload all nine CSVs to `/Volumes/workspace/default/raw_data/dataset/`.
2. Open the five `.py` files in the Databricks Workspace project folder `ecommerce-pyspark-databricks` and attach them to Serverless compute or another supported compute.
3. Run files `01` through `05` in order. Each stage depends on tables created by the previous stage.
4. Inspect the run output for errors and confirm the tables and CSVs exist before using them in a dashboard.

The scripts use overwrite writes. Rerunning the pipeline replaces the corresponding tables and exports. The Bronze ingestion timestamp therefore changes on a subsequent run.

### Post-run validation

Run this in a Databricks Python notebook attached to compute. It checks the expected Bronze counts reported above, confirms each Gold table has rows, and verifies that the five expected CSV files exist and are nonempty.

```python
expected_bronze_rows = {
    "orders": 99441,
    "order_items": 112650,
    "payments": 103886,
    "reviews": 99249,
    "customers": 99441,
    "sellers": 3095,
    "products": 32951,
    "geolocation": 1000163,
    "category_names": 71,
}

for table, expected in expected_bronze_rows.items():
    actual = spark.table(f"ecommerce_bronze.{table}").count()
    assert actual == expected, f"{table}: expected {expected}, got {actual}"

gold_tables = [
    "seller_performance",
    "monthly_revenue",
    "state_distribution",
    "late_delivery_heatmap",
    "category_performance",
]
export_path = "/Volumes/workspace/default/raw_data/tableau_exports/"

for table in gold_tables:
    rows = spark.table(f"ecommerce_gold.{table}").count()
    assert rows > 0, f"Empty Gold table: {table}"
    csv_path = f"{export_path}{table}.csv"
    files = dbutils.fs.ls(export_path)
    assert any(file.path == csv_path and file.size > 0 for file in files), (
        f"Missing or empty export: {csv_path}"
    )

print("Pipeline validation passed")
```

The validation snippet is provided for future runs; it was not run as a separate validation after the reported pipeline execution.

## Tableau

The README previously contained this Tableau Public link:

[Open the existing Olist E-Commerce Analytics story](https://public.tableau.com/views/BrazilianE-CommerceAnalytics/E-CommerceAnalyticsStory)

The link was not revalidated during the Databricks pipeline run. No new Tableau dashboard was created in that run. For a dashboard based on the CSV exports, first inspect the actual CSV schemas, then build revenue, seller, category, geography, and delivery visuals only from fields that are present. The exported files were reported in Databricks, but were not downloaded locally during the run.

## Screenshots

These project screenshots document the pipeline work:

| Image | Description |
|---|---|
| `screenshots/Catalog_view.png` | Databricks catalog view |
| `screenshots/Bronze_notebook.png` | Bronze ingestion |
| `screenshots/Silver_notebook.png` | Silver transformation |
| `screenshots/Gold_notebook.png` | Gold aggregation |
| `screenshots/architecture.png` | Pipeline architecture |

## Technology stack

| Area | Technology |
|---|---|
| Data processing | PySpark on Databricks |
| Storage | Delta Lake and Unity Catalog Volumes |
| Analytics | Databricks SQL |
| Visualization | Tableau Public (existing link; not revalidated in this run) |
| Source data | Olist Brazilian E-Commerce dataset |
