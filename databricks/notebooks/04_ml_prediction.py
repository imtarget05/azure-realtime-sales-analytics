# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — ML Prediction (Viral Classification)
# MAGIC
# MAGIC **Mục tiêu**: Load mô hình ML từ **MLflow Model Registry**,
# MAGIC dự đoán bài viết/transaction có **Viral** hay không (`is_viral_prediction`),
# MAGIC và ghi kết quả vào Gold layer.
# MAGIC
# MAGIC **Pipeline**:
# MAGIC ```
# MAGIC Gold (ml_features) ──► MLflow Model ──► Predictions ──► Gold (viral_predictions)
# MAGIC                         │                                    │
# MAGIC                         └── Registry: "sales-viral-classifier"
# MAGIC                             Stage: "Production"
# MAGIC ```
# MAGIC
# MAGIC **2 chế độ**:
# MAGIC - **Training**: Train model mới → log vào MLflow → register
# MAGIC - **Inference**: Load model đã register → predict batch

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import mlflow
import mlflow.sklearn
import mlflow.spark
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
import numpy as np

# COMMAND ----------

# Chế độ chạy: "train" hoặc "inference"
# Demo mode: luôn train trước rồi predict (không có model trong registry)
RUN_MODE = spark.conf.get("pipeline.ml_mode", "inference")
if DEMO_MODE:
    RUN_MODE = "train"
    print(f"ML Mode: {RUN_MODE} (demo — sẽ train inline rồi predict)")
else:
    print(f"ML Mode: {RUN_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Feature Data

# COMMAND ----------

features_df = spark.read.format("delta").load(f"{GOLD_PATH}/ml_features")

# Cột features cho model
NUMERIC_FEATURES = [
    "quantity", "unit_price", "discount", "total_amount",
    "temperature", "holiday", "is_weekend",
    "event_hour", "day_of_week", "event_month",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "store_avg_revenue", "store_txn_count",
    "product_total_qty", "price_deviation", "is_high_value",
    "similarity_score",
]

CATEGORICAL_FEATURES = ["store_id", "region", "category", "customer_segment"]
LABEL_COL = "is_viral"

print(f"✓ Features loaded: {features_df.count():,} rows")
print(f"  Numeric features:     {len(NUMERIC_FEATURES)}")
print(f"  Categorical features: {len(CATEGORICAL_FEATURES)}")
print(f"  Viral rate: {features_df.agg(F.mean(LABEL_COL)).first()[0]:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Prepare Features (Spark ML Pipeline)

# COMMAND ----------

# ── String Indexing cho categorical columns ──
indexers = [
    StringIndexer(
        inputCol=col, outputCol=f"{col}_idx",
        handleInvalid="keep"
    )
    for col in CATEGORICAL_FEATURES
]

indexed_features = NUMERIC_FEATURES + [f"{c}_idx" for c in CATEGORICAL_FEATURES]

assembler = VectorAssembler(
    inputCols=indexed_features,
    outputCol="features",
    handleInvalid="skip"
)

prep_pipeline = Pipeline(stages=indexers + [assembler])
prep_model = prep_pipeline.fit(features_df)
prepared_df = prep_model.transform(features_df)

print(f"✓ Feature vector assembled: {len(indexed_features)} dimensions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3A. TRAINING MODE — Train & Register Model

# COMMAND ----------

if RUN_MODE == "train":
    from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

    # ── Train/Test split ──
    train_df, test_df = prepared_df.randomSplit([0.8, 0.2], seed=42)

    print(f"  Train: {train_df.count():,} rows (viral: {train_df.filter(F.col(LABEL_COL)==1).count():,})")
    print(f"  Test:  {test_df.count():,} rows (viral: {test_df.filter(F.col(LABEL_COL)==1).count():,})")

    # ── MLflow experiment ──
    mlflow.set_experiment("/Experiments/sales-viral-prediction")

    with mlflow.start_run(run_name="gbt_viral_classifier") as run:
        # ── Gradient Boosted Trees Classifier ──
        gbt = GBTClassifier(
            labelCol=LABEL_COL,
            featuresCol="features",
            maxIter=100,
            maxDepth=6,
            stepSize=0.1,
            subsamplingRate=0.8,
            featureSubsetStrategy="sqrt",
            seed=42,
        )

        # ── Cross-validation ──
        evaluator = BinaryClassificationEvaluator(
            labelCol=LABEL_COL,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )

        param_grid = (
            ParamGridBuilder()
            .addGrid(gbt.maxDepth, [4, 6, 8])
            .addGrid(gbt.maxIter, [50, 100])
            .build()
        )

        cv = CrossValidator(
            estimator=gbt,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            parallelism=4,
            seed=42,
        )

        # ── Fit ──
        print("  Training GBT Classifier with CrossValidation...")
        cv_model = cv.fit(train_df)
        best_model = cv_model.bestModel

        # ── Evaluate ──
        predictions = cv_model.transform(test_df)
        auc_roc = evaluator.evaluate(predictions)

        mc_evaluator = MulticlassClassificationEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction"
        )
        accuracy = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "accuracy"})
        f1 = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "f1"})
        precision = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedPrecision"})
        recall = mc_evaluator.evaluate(predictions, {mc_evaluator.metricName: "weightedRecall"})

        # ── Log metrics ──
        mlflow.log_param("model_type", "GBTClassifier")
        mlflow.log_param("max_depth", best_model.getMaxDepth())
        mlflow.log_param("max_iter", best_model.getMaxIter)
        mlflow.log_param("num_features", len(indexed_features))
        mlflow.log_param("train_size", train_df.count())
        mlflow.log_param("test_size", test_df.count())

        mlflow.log_metric("auc_roc", auc_roc)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)

        # ── Log model ──
        mlflow.spark.log_model(
            cv_model,
            artifact_path="viral_classifier",
            registered_model_name=MLFLOW_MODEL_NAME,
        )

        # ── Log feature importance ──
        importance = best_model.featureImportances.toArray()
        feature_importance = sorted(
            zip(indexed_features, importance),
            key=lambda x: x[1],
            reverse=True
        )

        print(f"\n{'='*60}")
        print(f"  MODEL TRAINING COMPLETE")
        print(f"{'='*60}")
        print(f"  AUC-ROC:   {auc_roc:.4f}")
        print(f"  Accuracy:  {accuracy:.4f}")
        print(f"  F1 Score:  {f1:.4f}")
        print(f"  Precision: {precision:.4f}")
        print(f"  Recall:    {recall:.4f}")
        print(f"\n  Top 10 Features:")
        for feat, imp in feature_importance[:10]:
            print(f"    {feat:<30} {imp:.4f}")
        print(f"\n  MLflow Run ID: {run.info.run_id}")
        print(f"  Model registered: {MLFLOW_MODEL_NAME}")

    # ── Demo mode: cũng chạy predict sau khi train ──
    if DEMO_MODE:
        print("\n[Demo] Running predictions with freshly trained model...")
        predictions = cv_model.transform(prepared_df)
        result_df = (
            predictions
            .withColumn("is_viral_prediction", F.col("prediction").cast(IntegerType()))
            .withColumn(
                "viral_probability",
                F.round(F.element_at(F.col("probability"), 2), 4)
            )
            .select(
                "transaction_id", "event_timestamp", "event_date",
                "store_id", "region", "product_id", "product_name", "category",
                "customer_id", "customer_segment",
                "quantity", "unit_price", "total_amount",
                "similarity_score", "similarity_bin",
                "is_viral",
                "is_viral_prediction",
                "viral_probability",
            )
        )
        print(f"✓ Demo predictions: {result_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3B. INFERENCE MODE — Load Model & Predict

# COMMAND ----------

if RUN_MODE == "inference":
    # ── Load model từ MLflow Registry ──
    model_uri = f"models:/{MLFLOW_MODEL_NAME}/{MLFLOW_MODEL_STAGE}"

    print(f"Loading model: {model_uri}")
    loaded_model = mlflow.spark.load_model(model_uri)

    # ── Predict ──
    predictions = loaded_model.transform(prepared_df)

    # ── Extract prediction columns ──
    result_df = (
        predictions
        .withColumn("is_viral_prediction", F.col("prediction").cast(IntegerType()))
        .withColumn(
            "viral_probability",
            # GBT probability: cột thứ 1 của probability vector
            F.round(
                F.element_at(F.col("probability"), 2),  # P(viral=1)
                4
            )
        )
        .select(
            # Original columns
            "transaction_id", "event_timestamp", "event_date",
            "store_id", "region", "product_id", "product_name", "category",
            "customer_id", "customer_segment",
            "quantity", "unit_price", "total_amount",
            "similarity_score", "similarity_bin",
            "is_viral",  # actual label

            # Predictions
            "is_viral_prediction",
            "viral_probability",
        )
    )

    print(f"✓ Predictions generated: {result_df.count():,} rows")
    print(f"  Predicted viral: {result_df.filter(F.col('is_viral_prediction')==1).count():,}")
    print(f"  Predicted non-viral: {result_df.filter(F.col('is_viral_prediction')==0).count():,}")

    # ── Confusion matrix ──
    cm = (
        result_df
        .groupBy("is_viral", "is_viral_prediction")
        .count()
        .orderBy("is_viral", "is_viral_prediction")
    )
    print("\n  Confusion Matrix:")
    cm.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Save Predictions to Gold Layer

# COMMAND ----------

if RUN_MODE == "inference" or DEMO_MODE:
    (
        result_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(GOLD_VIRAL_PATH)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DB}.viral_predictions
        USING DELTA
        LOCATION '{GOLD_VIRAL_PATH}'
    """)

    try:
        spark.sql(f"OPTIMIZE delta.`{GOLD_VIRAL_PATH}` ZORDER BY (store_id, category)")
    except Exception as e:
        print(f"⚠ OPTIMIZE skipped: {e}")

    print(f"✓ Predictions saved: {GOLD_DB}.viral_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Model Performance Monitoring

# COMMAND ----------

if RUN_MODE == "inference" or DEMO_MODE:
    accuracy = result_df.filter(
        F.col("is_viral") == F.col("is_viral_prediction")
    ).count() / result_df.count()

    viral_precision = result_df.filter(
        (F.col("is_viral_prediction") == 1) & (F.col("is_viral") == 1)
    ).count() / max(
        result_df.filter(F.col("is_viral_prediction") == 1).count(), 1
    )

    viral_recall = result_df.filter(
        (F.col("is_viral_prediction") == 1) & (F.col("is_viral") == 1)
    ).count() / max(
        result_df.filter(F.col("is_viral") == 1).count(), 1
    )

    print(f"""
╔══════════════════════════════════════════════════════╗
║  MODEL PERFORMANCE (Live Data)                       ║
╠══════════════════════════════════════════════════════╣
║  Accuracy:       {accuracy:.4f}                      ║
║  Viral Precision:{viral_precision:.4f}               ║
║  Viral Recall:   {viral_recall:.4f}                  ║
║  Total Predicted Viral: {result_df.filter(F.col('is_viral_prediction')==1).count():>8,}          ║
╚══════════════════════════════════════════════════════╝
    """)

    # Log metrics vào MLflow cho monitoring drift
    with mlflow.start_run(run_name="inference_monitoring"):
        mlflow.log_metric("live_accuracy", accuracy)
        mlflow.log_metric("live_viral_precision", viral_precision)
        mlflow.log_metric("live_viral_recall", viral_recall)
        mlflow.log_metric("live_total_rows", result_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **ML Prediction hoàn tất.**
# MAGIC - Training mode: GBT Classifier + CrossValidation → MLflow Registry
# MAGIC - Inference mode: Load Production model → batch predict → Gold Delta
# MAGIC - Tiếp tục: `05_gold_aggregation.py` cho bảng tổng hợp BI
