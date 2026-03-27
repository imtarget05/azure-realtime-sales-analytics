# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Feature Engineering (Silver → ML Features)
# MAGIC
# MAGIC **Mục tiêu**: Tiền xử lý văn bản (Text NLP), tính toán Feature cho ML,
# MAGIC bao gồm **Similarity Score** giữa các bài đăng/sản phẩm.
# MAGIC
# MAGIC **Pipeline**:
# MAGIC ```
# MAGIC Silver (sales_transactions)
# MAGIC    │
# MAGIC    ├── Text features (TF-IDF trên product_name + category)
# MAGIC    ├── Numerical features (quantity, price, time...)
# MAGIC    ├── Similarity Score (cosine similarity giữa transactions)
# MAGIC    └── Viral label (revenue outlier detection)
# MAGIC    │
# MAGIC    ▼
# MAGIC Feature Table (Gold) → MLflow Training → Prediction
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, ArrayType, FloatType, StringType, IntegerType
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver Data

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_PATH)

print(f"✓ Silver loaded: {silver_df.count():,} rows")
print(f"  Columns: {len(silver_df.columns)}")
print(f"  Date range: {silver_df.agg(F.min('event_date'), F.max('event_date')).first()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Text Feature Engineering (NLP)
# MAGIC
# MAGIC Tạo text features từ `product_name`, `category`, `weather` sử dụng
# MAGIC **TF-IDF vectorization** (Spark MLlib) để:
# MAGIC - Biểu diễn sản phẩm dưới dạng vector
# MAGIC - Tính **Cosine Similarity** giữa các transactions
# MAGIC - Phục vụ recommendation & clustering

# COMMAND ----------

from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF,
    StringIndexer, OneHotEncoder, VectorAssembler
)
from pyspark.ml import Pipeline

# ── 2a. Tạo cột text tổng hợp ──
text_df = (
    silver_df
    .withColumn(
        "text_combined",
        F.concat_ws(" ",
            F.lower(F.col("product_name")),
            F.lower(F.col("category")),
            F.lower(F.col("weather")),
            F.col("region"),
        )
    )
    .withColumn(
        "text_combined",
        F.regexp_replace(F.col("text_combined"), r"[^a-z0-9\s]", "")
    )
)

# ── 2b. TF-IDF Pipeline ──
tokenizer = Tokenizer(inputCol="text_combined", outputCol="tokens")
remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens")
hashing_tf = HashingTF(inputCol="filtered_tokens", outputCol="raw_features", numFeatures=256)
idf = IDF(inputCol="raw_features", outputCol="tfidf_features")

text_pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf])
text_model = text_pipeline.fit(text_df)
tfidf_df = text_model.transform(text_df)

print("✓ TF-IDF features computed (256 dimensions)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Similarity Score (Cosine Similarity)
# MAGIC
# MAGIC Tính **Cosine Similarity** giữa mỗi transaction và centroid (trung bình)
# MAGIC của cùng `category`. Điểm cao → sản phẩm "điển hình" của danh mục,
# MAGIC điểm thấp → sản phẩm "khác biệt" (tiềm năng viral/outlier).

# COMMAND ----------

from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.ml.feature import Normalizer

# ── 3a. Normalize TF-IDF vectors (L2 norm) ──
normalizer = Normalizer(inputCol="tfidf_features", outputCol="norm_features", p=2.0)
norm_df = normalizer.transform(tfidf_df)

# ── 3b. Tính centroid per category ──
# Chuyển sang Pandas để tính mean vector (Spark native khó xử lý sparse vector)
@F.udf(returnType=ArrayType(FloatType()))
def vector_to_array(v):
    """Convert Spark MLlib vector → Python list."""
    if v is None:
        return None
    return [float(x) for x in v.toArray()]

norm_with_array = norm_df.withColumn("feature_array", vector_to_array(F.col("norm_features")))

# Tính centroid bằng cách average mỗi element theo category
# Sử dụng aggregate trên array
centroid_df = (
    norm_with_array
    .groupBy("category")
    .agg(
        # Mean của mỗi dimension trong feature array
        F.expr("""
            transform(
                sequence(0, 255),
                i -> avg(feature_array[i])
            )
        """).alias("centroid_array")
    )
)

# ── 3c. Join centroid và tính cosine similarity ──
similarity_df = norm_with_array.join(centroid_df, on="category", how="left")

@F.udf(returnType=DoubleType())
def cosine_similarity(vec_a, vec_b):
    """Cosine similarity giữa 2 arrays."""
    if vec_a is None or vec_b is None:
        return 0.0
    a = np.array(vec_a, dtype=np.float64)
    b = np.array(vec_b, dtype=np.float64)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))

similarity_df = similarity_df.withColumn(
    "similarity_score",
    F.round(cosine_similarity(F.col("feature_array"), F.col("centroid_array")), 4)
)

# ── 3d. Bin similarity scores (0.0, 0.1, 0.2, ..., 1.0) ──
similarity_df = similarity_df.withColumn(
    "similarity_bin",
    F.round(F.col("similarity_score"), 1)
)

print("✓ Similarity scores computed")
print(f"  Score distribution:")
similarity_df.groupBy("similarity_bin").count().orderBy("similarity_bin").show(11)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Numerical Feature Engineering

# COMMAND ----------

feature_df = (
    similarity_df

    # ── 4a. Revenue features per store (rolling) ──
    .withColumn(
        "store_avg_revenue",
        F.avg("total_amount").over(
            Window.partitionBy("store_id")
            .orderBy("event_timestamp")
            .rowsBetween(-100, 0)
        )
    )
    .withColumn(
        "store_txn_count",
        F.count("*").over(
            Window.partitionBy("store_id")
            .orderBy("event_timestamp")
            .rowsBetween(-100, 0)
        )
    )

    # ── 4b. Product popularity (global) ──
    .withColumn(
        "product_total_qty",
        F.sum("quantity").over(Window.partitionBy("product_id"))
    )

    # ── 4c. Hour-of-day sin/cos encoding (cyclical) ──
    .withColumn("hour_sin", F.sin(2 * np.pi * F.col("event_hour") / 24))
    .withColumn("hour_cos", F.cos(2 * np.pi * F.col("event_hour") / 24))

    # ── 4d. Day-of-week sin/cos encoding ──
    .withColumn("dow_sin", F.sin(2 * np.pi * F.col("day_of_week") / 7))
    .withColumn("dow_cos", F.cos(2 * np.pi * F.col("day_of_week") / 7))

    # ── 4e. Price deviation from category average ──
    .withColumn(
        "cat_avg_price",
        F.avg("unit_price").over(Window.partitionBy("category"))
    )
    .withColumn(
        "price_deviation",
        F.round((F.col("unit_price") - F.col("cat_avg_price")) / F.col("cat_avg_price"), 4)
    )

    # ── 4f. Is high value transaction ──
    .withColumn(
        "is_high_value",
        F.when(F.col("total_amount") > F.lit(500), 1).otherwise(0)
    )
)

print("✓ Numerical features computed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Viral Label Generation
# MAGIC
# MAGIC Định nghĩa **"viral"**: transaction có `total_amount` > P95 của category,
# MAGIC hoặc `quantity` > P95 toàn bộ → **is_viral = 1**.
# MAGIC Label này dùng để train classification model.

# COMMAND ----------

# Tính percentile P95 per category
p95_df = (
    feature_df
    .groupBy("category")
    .agg(
        F.percentile_approx("total_amount", 0.95).alias("p95_amount"),
        F.percentile_approx("quantity", 0.95).alias("p95_qty"),
    )
)

feature_with_label = (
    feature_df
    .join(p95_df, on="category", how="left")
    .withColumn(
        "is_viral",
        F.when(
            (F.col("total_amount") > F.col("p95_amount")) |
            (F.col("quantity") > F.col("p95_qty")),
            1
        ).otherwise(0)
    )
)

viral_rate = feature_with_label.agg(F.mean("is_viral")).first()[0]
print(f"✓ Viral labels generated")
print(f"  Viral rate: {viral_rate:.2%}")
print(f"  Viral count: {feature_with_label.filter(F.col('is_viral') == 1).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Save Feature Table

# COMMAND ----------

# Chọn cột cuối cùng cho ML
ml_feature_columns = [
    # Keys
    "transaction_id", "event_timestamp", "event_date",
    "store_id", "region", "product_id", "product_name", "category",
    "customer_id", "customer_segment",

    # Raw features
    "quantity", "unit_price", "discount", "total_amount",
    "temperature", "holiday", "is_weekend",
    "event_hour", "day_of_week", "event_month",

    # Engineered
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "store_avg_revenue", "store_txn_count",
    "product_total_qty", "price_deviation", "is_high_value",

    # NLP
    "similarity_score", "similarity_bin",

    # Label
    "is_viral",
]

ml_features = feature_with_label.select(*ml_feature_columns)

# Lưu vào Gold layer
(
    ml_features.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(f"{GOLD_PATH}/ml_features")
)

# Đăng ký table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_DB}.ml_features
    USING DELTA
    LOCATION '{GOLD_PATH}/ml_features'
""")

spark.sql(f"OPTIMIZE delta.`{GOLD_PATH}/ml_features` ZORDER BY (category, store_id)")

print(f"✓ Feature table saved: {GOLD_DB}.ml_features")
print(f"  Shape: {ml_features.count():,} rows × {len(ml_feature_columns)} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save Similarity Scores (cho BI)

# COMMAND ----------

# Bảng riêng cho Power BI: similarity distribution
sim_summary = (
    feature_with_label
    .groupBy("category", "similarity_bin")
    .agg(
        F.count("*").alias("txn_count"),
        F.round(F.avg("total_amount"), 2).alias("avg_amount"),
        F.sum("is_viral").alias("viral_count"),
    )
    .orderBy("category", "similarity_bin")
)

(
    sim_summary.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_SIMILARITY_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_DB}.similarity_scores
    USING DELTA
    LOCATION '{GOLD_SIMILARITY_PATH}'
""")

print(f"✓ Similarity scores saved: {GOLD_DB}.similarity_scores")
sim_summary.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Feature Engineering hoàn tất.**
# MAGIC - Feature table sẵn sàng cho MLflow training (`04_ml_prediction.py`)
# MAGIC - Similarity scores sẵn sàng cho Power BI
