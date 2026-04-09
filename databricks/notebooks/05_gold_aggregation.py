# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold Aggregation (Bảng tổng hợp cho SaaS / BI)
# MAGIC
# MAGIC **Mục tiêu**: Tạo các bảng **Aggregated** tối ưu cho:
# MAGIC - Power BI DirectQuery (low latency)
# MAGIC - Serverless SQL Warehouse (cost-effective)
# MAGIC - SaaS application API (pre-computed KPIs)
# MAGIC
# MAGIC **Bảng Gold**:
# MAGIC ```
# MAGIC Gold Layer
# MAGIC ├── hourly_summary       ← Revenue/orders per hour × store × category
# MAGIC ├── product_summary      ← Product performance + similarity
# MAGIC ├── customer_summary     ← Customer lifetime value + segments
# MAGIC ├── viral_predictions    ← ML predictions (từ notebook 04)
# MAGIC └── similarity_scores    ← Similarity bins (từ notebook 03)
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver & Predictions

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_PATH)

# Load predictions nếu có
HAS_PREDICTIONS = False
try:
    viral_df = spark.read.format("delta").load(GOLD_VIRAL_PATH)
    HAS_PREDICTIONS = True
    print(f"✓ Viral predictions loaded: {viral_df.count():,} rows")
except Exception:
    print("⚠ No viral predictions found — using Silver only")

print(f"✓ Silver loaded: {silver_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold Table: Hourly Summary
# MAGIC
# MAGIC Bảng tổng hợp doanh thu theo **giờ × cửa hàng × danh mục**.
# MAGIC Tối ưu cho Power BI time-series charts & KPI cards.

# COMMAND ----------

hourly_summary = (
    silver_df
    .groupBy(
        "event_date",
        "event_hour",
        "store_id",
        "region",
        "category",
    )
    .agg(
        # Revenue KPIs
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("transaction_id").alias("order_count"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.round(F.sum("quantity"), 0).alias("total_quantity"),

        # Product diversity
        F.countDistinct("product_id").alias("unique_products"),
        F.countDistinct("customer_id").alias("unique_customers"),

        # Context
        F.round(F.avg("temperature"), 1).alias("avg_temperature"),
        F.first("weather").alias("dominant_weather"),
        F.max("holiday").alias("is_holiday"),

        # Discount impact
        F.round(F.avg("discount"), 4).alias("avg_discount"),
        F.round(
            F.sum(F.col("quantity") * F.col("unit_price") * F.col("discount")),
            2
        ).alias("total_discount_amount"),
    )

    # Thêm timestamp cho charting
    .withColumn(
        "hour_timestamp",
        F.to_timestamp(
            F.concat_ws(" ",
                F.col("event_date").cast("string"),
                F.lpad(F.col("event_hour").cast("string"), 2, "0")
            ),
            "yyyy-MM-dd HH"
        )
    )

    # Revenue per customer
    .withColumn(
        "revenue_per_customer",
        F.round(F.col("total_revenue") / F.greatest(F.col("unique_customers"), F.lit(1)), 2)
    )
)

# ── Save ──
(
    hourly_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(GOLD_HOURLY_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_DB}.hourly_summary
    USING DELTA
    LOCATION '{GOLD_HOURLY_PATH}'
""")

spark.sql(f"OPTIMIZE delta.`{GOLD_HOURLY_PATH}` ZORDER BY (store_id, event_hour)")

print(f"✓ Hourly summary saved: {hourly_summary.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold Table: Product Summary
# MAGIC
# MAGIC Tổng hợp hiệu suất sản phẩm + similarity scores.

# COMMAND ----------

# Load similarity nếu có
HAS_SIM = False
try:
    sim_df = spark.read.format("delta").load(GOLD_SIMILARITY_PATH)
    HAS_SIM = True
except Exception:
    HAS_SIM = False

product_summary = (
    silver_df
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("*").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_price"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_buyers"),
        F.countDistinct("store_id").alias("stores_sold"),
        F.min("event_date").alias("first_sale_date"),
        F.max("event_date").alias("last_sale_date"),
    )
    # Revenue rank within category
    .withColumn(
        "category_rank",
        F.rank().over(
            Window.partitionBy("category")
            .orderBy(F.col("total_revenue").desc())
        )
    )
    # Days active
    .withColumn(
        "days_active",
        F.datediff(F.col("last_sale_date"), F.col("first_sale_date")) + 1
    )
    # Orders per day
    .withColumn(
        "orders_per_day",
        F.round(F.col("total_orders") / F.greatest(F.col("days_active"), F.lit(1)), 2)
    )
)

# Join viral prediction rate per product
if HAS_PREDICTIONS:
    viral_by_product = (
        viral_df
        .groupBy("product_id")
        .agg(
            F.round(F.mean("is_viral_prediction"), 4).alias("viral_rate"),
            F.round(F.avg("viral_probability"), 4).alias("avg_viral_prob"),
        )
    )
    product_summary = product_summary.join(viral_by_product, on="product_id", how="left")
    product_summary = product_summary.fillna({"viral_rate": 0.0, "avg_viral_prob": 0.0})

# ── Save ──
(
    product_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_PRODUCT_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_DB}.product_summary
    USING DELTA
    LOCATION '{GOLD_PRODUCT_PATH}'
""")

print(f"✓ Product summary saved: {product_summary.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Table: Customer Summary
# MAGIC
# MAGIC Customer Lifetime Value (CLV) và phân khúc để phục vụ:
# MAGIC - Top 5 Customers by Revenue (Power BI)
# MAGIC - RLS filtering theo region
# MAGIC - SaaS customer dashboard

# COMMAND ----------

customer_summary = (
    silver_df
    .groupBy("customer_id", "customer_segment", "region")
    .agg(
        # Revenue
        F.round(F.sum("total_amount"), 2).alias("total_spent"),
        F.count("*").alias("total_orders"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.sum("quantity").alias("total_items"),

        # Recency
        F.max("event_date").alias("last_purchase_date"),
        F.min("event_date").alias("first_purchase_date"),

        # Diversity
        F.countDistinct("product_id").alias("unique_products_bought"),
        F.countDistinct("category").alias("unique_categories"),
        F.countDistinct("store_id").alias("stores_visited"),

        # Favorite
        F.first("payment_method").alias("preferred_payment"),
    )
    # Customer tenure
    .withColumn(
        "tenure_days",
        F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date")) + 1
    )
    # Purchase frequency
    .withColumn(
        "purchase_frequency",
        F.round(F.col("total_orders") / F.greatest(F.col("tenure_days"), F.lit(1)), 4)
    )
    # CLV tier
    .withColumn(
        "clv_tier",
        F.when(F.col("total_spent") >= 10000, "Platinum")
         .when(F.col("total_spent") >= 5000,  "Gold")
         .when(F.col("total_spent") >= 1000,  "Silver")
         .otherwise("Bronze")
    )
    # Rank by revenue (global)
    .withColumn(
        "revenue_rank",
        F.rank().over(Window.orderBy(F.col("total_spent").desc()))
    )
    # Rank by revenue within region (cho RLS)
    .withColumn(
        "region_revenue_rank",
        F.rank().over(
            Window.partitionBy("region")
            .orderBy(F.col("total_spent").desc())
        )
    )
)

# ── Save ──
(
    customer_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("region")
    .save(GOLD_CUSTOMER_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_DB}.customer_summary
    USING DELTA
    LOCATION '{GOLD_CUSTOMER_PATH}'
""")

spark.sql(f"OPTIMIZE delta.`{GOLD_CUSTOMER_PATH}` ZORDER BY (customer_id)")

print(f"✓ Customer summary saved: {customer_summary.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Serverless SQL Views
# MAGIC
# MAGIC Tạo SQL Views tối ưu cho Serverless SQL Warehouse —
# MAGIC Power BI sẽ query trực tiếp views này qua DirectQuery.

# COMMAND ----------

# ── View: KPI Dashboard (pre-aggregated) ──
spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_DB}.v_kpi_dashboard AS
    SELECT
        event_date,
        SUM(total_revenue)                                    AS revenue_total,
        SUM(order_count)                                      AS order_total,
        ROUND(SUM(total_revenue) / NULLIF(SUM(order_count), 0), 2) AS avg_order_value,
        SUM(total_quantity)                                   AS units_sold,
        SUM(unique_customers)                                 AS active_customers,
        SUM(total_discount_amount)                            AS total_discounts
    FROM {GOLD_DB}.hourly_summary
    GROUP BY event_date
    ORDER BY event_date DESC
""")

# ── View: Top 5 Customers by Revenue (per region, per time range) ──
spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_DB}.v_top_customers AS
    SELECT
        customer_id,
        customer_segment,
        region,
        total_spent,
        total_orders,
        avg_order_value,
        clv_tier,
        revenue_rank,
        region_revenue_rank,
        last_purchase_date
    FROM {GOLD_DB}.customer_summary
    WHERE revenue_rank <= 100
    ORDER BY total_spent DESC
""")

# ── View: Product Performance + Viral Rate ──
_viral_cols = "viral_rate, avg_viral_prob," if HAS_PREDICTIONS else ""
spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_DB}.v_product_performance AS
    SELECT
        product_id,
        product_name,
        category,
        total_revenue,
        total_orders,
        avg_price,
        category_rank,
        unique_buyers,
        {_viral_cols}
        orders_per_day
    FROM {GOLD_DB}.product_summary
    ORDER BY total_revenue DESC
""")

# ── View: Similarity Distribution (cho DAX bins) ──
spark.sql(f"""
    CREATE OR REPLACE VIEW {GOLD_DB}.v_similarity_distribution AS
    SELECT
        category,
        similarity_bin,
        txn_count,
        avg_amount,
        viral_count,
        ROUND(100.0 * viral_count / NULLIF(txn_count, 0), 2) AS viral_pct
    FROM {GOLD_DB}.similarity_scores
    ORDER BY category, similarity_bin
""")

print("✓ All SQL Views created:")
print(f"  - {GOLD_DB}.v_kpi_dashboard")
print(f"  - {GOLD_DB}.v_top_customers")
print(f"  - {GOLD_DB}.v_product_performance")
print(f"  - {GOLD_DB}.v_similarity_distribution")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality Summary

# COMMAND ----------

print(f"""
╔══════════════════════════════════════════════════════════════╗
║  GOLD LAYER SUMMARY                                         ║
╠══════════════════════════════════════════════════════════════╣
║  hourly_summary:    {spark.read.format("delta").load(GOLD_HOURLY_PATH).count():>10,} rows  ║
║  product_summary:   {spark.read.format("delta").load(GOLD_PRODUCT_PATH).count():>10,} rows  ║
║  customer_summary:  {spark.read.format("delta").load(GOLD_CUSTOMER_PATH).count():>10,} rows  ║
║  viral_predictions: {'N/A':>10}       ║
║  similarity_scores: {'N/A':>10}       ║
╠══════════════════════════════════════════════════════════════╣
║  SQL Views: 4 views ready for Power BI DirectQuery          ║
║  Partitioned by: event_date (hourly), region (customer)     ║
║  Z-Ordered by: store_id, event_hour, customer_id            ║
╚══════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Gold Aggregation hoàn tất.**
# MAGIC
# MAGIC Bước tiếp theo: Giai đoạn 3 — Data Modeling & DAX (Power BI)
# MAGIC - Kết nối DirectQuery từ Power BI → Serverless SQL
# MAGIC - DAX measures cho Revenue, AOV, Top Customers
# MAGIC - Row-Level Security (Dynamic RLS)
