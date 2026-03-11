# Kế hoạch Phát triển MLOps

> **Đồ án:** Hệ thống trực quan dữ liệu bán hàng thời gian thực trên Azure

---

## 1. Tổng quan: Từ ML → MLOps

### Hiện trạng (ML thủ công)

```
Developer ──► Train local ──► Deploy thủ công ──► Endpoint ──► Không giám sát
                  │                  │
                  └── Không version ──┘── Không auto-retrain
```

### Mục tiêu (MLOps Level 2)

```
Data Pipeline ──► Auto Training ──► Model Registry ──► Auto Deploy ──► Monitoring
     │                  │                 │                  │              │
     ▼                  ▼                 ▼                  ▼              ▼
  Event Hub       AML Pipeline      Versioning +       Blue/Green      Data Drift
  + SQL DB        + Compute          Approval          Deployment      + Retrain
                  Cluster            Gate               via CI/CD       Trigger
```

### MLOps Maturity Model

| Level | Mô tả | Hiện tại | Mục tiêu |
|-------|--------|----------|----------|
| **0** | Không có MLOps, train thủ công | ✅ Đang ở đây | |
| **1** | Automated training, manual deploy | | |
| **2** | CI/CD cho cả code + model | | ✅ Mục tiêu |
| **3** | Full automation + monitoring + auto-retrain | | Hướng phát triển |

---

## 2. Kiến trúc MLOps trên Azure

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AZURE DEVOPS / GITHUB ACTIONS                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────────┐   │
│  │ Code Push│──▶│ CI: Test  │──▶│ CD: Deploy│──▶│ Release Gate     │   │
│  │ (Git)    │   │ + Lint    │   │ Model    │   │ (Approval/Auto)  │   │
│  └──────────┘   └──────────┘   └──────────┘   └──────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
         │                              │                    │
         ▼                              ▼                    ▼
┌─────────────────┐  ┌──────────────────────┐  ┌─────────────────────┐
│  Azure ML       │  │  Model Registry      │  │  Online Endpoint    │
│  Pipeline       │  │                      │  │                     │
│                 │  │  v1 (R²=0.85) ❌     │  │  Blue: v2 (90%)    │
│  Step 1: Data   │  │  v2 (R²=0.92) ✅     │  │  Green: v3 (10%)   │
│  Step 2: Train  │  │  v3 (R²=0.94) 🔄    │  │                     │
│  Step 3: Eval   │  │                      │  │  Auto-scale 1-5     │
│  Step 4: Register│  └──────────────────────┘  └─────────────────────┘
└─────────────────┘                                       │
         ▲                                                ▼
         │                                    ┌─────────────────────┐
         │                                    │  Azure Monitor      │
         │                                    │                     │
         └────────── Retrain Trigger ◄────────│  • Data Drift       │
                                              │  • Model Accuracy   │
                                              │  • Latency/Errors   │
                                              └─────────────────────┘
```

---

## 3. Lộ trình triển khai (4 Phase)

### Phase 1: ML Pipeline Automation (Tuần 1-2)

**Mục tiêu:** Chuyển training thủ công thành Azure ML Pipeline tự động

#### 3.1.1 Tạo Azure ML Pipeline

| Component | File | Mô tả |
|-----------|------|--------|
| Data Preparation Step | `mlops/pipeline/data_prep.py` | Query SQL → clean → feature engineering |
| Training Step | `mlops/pipeline/train_step.py` | Train model với hyperparameter tuning |
| Evaluation Step | `mlops/pipeline/evaluate.py` | So sánh model mới vs model đang production |
| Registration Step | `mlops/pipeline/register.py` | Register model nếu metrics tốt hơn |
| Pipeline Definition | `mlops/pipeline/pipeline.py` | Kết nối 4 steps thành pipeline |

#### 3.1.2 Chi tiết Pipeline

```python
# mlops/pipeline/pipeline.py (tóm tắt)
from azure.ai.ml.dsl import pipeline
from azure.ai.ml import Input

@pipeline(description="Sales Forecast Training Pipeline")
def sales_forecast_pipeline(training_data: Input):
    # Step 1: Chuẩn bị dữ liệu
    data_prep = data_prep_component(raw_data=training_data)

    # Step 2: Train với HyperDrive (auto-tune hyperparameters)
    train = train_component(
        train_data=data_prep.outputs.train_data,
        val_data=data_prep.outputs.val_data,
        n_estimators=Choice([100, 200, 300]),
        max_depth=Choice([3, 5, 7, 10]),
        learning_rate=Uniform(0.01, 0.3),
    )

    # Step 3: Evaluate vs production model
    evaluate = eval_component(
        model=train.outputs.model,
        test_data=data_prep.outputs.test_data,
        production_model="sales-forecast-model:latest",
    )

    # Step 4: Register nếu tốt hơn
    register = register_component(
        model=train.outputs.model,
        metrics=evaluate.outputs.metrics,
        should_register=evaluate.outputs.is_better,
    )

    return {"model": register.outputs.registered_model}
```

#### 3.1.3 Evaluation Gate Logic

```python
# Chỉ register model mới nếu thỏa ĐỦ các điều kiện:
PROMOTION_CRITERIA = {
    "R2":        {"min": 0.85, "direction": "higher_is_better"},
    "MAE":       {"max": 15.0, "direction": "lower_is_better"},
    "MAPE_%":    {"max": 12.0, "direction": "lower_is_better"},
    "improvement_vs_production_%": {"min": 2.0},  # phải tốt hơn ít nhất 2%
}
```

#### 3.1.4 Schedule

```python
# Chạy pipeline training tự động mỗi tuần (Chủ nhật 03:00 UTC)
from azure.ai.ml.entities import RecurrenceTrigger

schedule = RecurrenceTrigger(
    frequency="week",
    interval=1,
    schedule=RecurrencePattern(
        week_days=["Sunday"],
        hours=[3],
        minutes=[0],
    ),
)
```

**Deliverables Phase 1:**
- [ ] Azure ML Pipeline 4 steps chạy được
- [ ] HyperDrive auto-tune hyperparameters
- [ ] Evaluation gate: chỉ register model tốt hơn production
- [ ] Weekly schedule trigger

---

### Phase 2: CI/CD Pipeline (Tuần 3-4)

**Mục tiêu:** Tự động test, build, deploy khi code thay đổi

#### 3.2.1 Cấu trúc CI/CD

```
┌──────────────────────────────────────────────────────────────┐
│                    GITHUB ACTIONS WORKFLOWS                  │
│                                                              │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────┐  │
│  │ ci.yml      │    │ cd-model.yml │    │ cd-webapp.yml  │  │
│  │             │    │              │    │                │  │
│  │ On: push    │    │ On: model    │    │ On: push to    │  │
│  │ to main     │    │ registered   │    │ webapp/        │  │
│  │             │    │              │    │                │  │
│  │ • Lint      │    │ • Pull model │    │ • Build Docker │  │
│  │ • Unit test │    │ • Test endpt │    │ • Push to ACR  │  │
│  │ • Trigger   │──▶ │ • Blue/Green │    │ • Deploy App   │  │
│  │   training  │    │ • Smoke test │    │   Service      │  │
│  └─────────────┘    └──────────────┘    └────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

#### 3.2.2 CI Pipeline (`ci.yml`)

```yaml
# .github/workflows/ci.yml
name: CI - Test & Validate

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements.txt pytest pytest-cov flake8

      - name: Lint
        run: flake8 ml/ webapp/ benchmarks/ --max-line-length=120

      - name: Unit Tests
        run: pytest tests/ -v --cov=ml --cov-report=xml

      - name: Validate ML Pipeline
        run: python -c "from mlops.pipeline.pipeline import sales_forecast_pipeline; print('Pipeline valid')"

  trigger-training:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - name: Trigger Azure ML Pipeline
        uses: azure/login@v2
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}

      - name: Submit Pipeline Run
        run: |
          az ml job create \
            --file mlops/pipeline/pipeline_job.yml \
            --resource-group ${{ secrets.AZURE_RESOURCE_GROUP }} \
            --workspace-name ${{ secrets.AML_WORKSPACE_NAME }}
```

#### 3.2.3 CD Model Pipeline (`cd-model.yml`)

```yaml
# .github/workflows/cd-model.yml
name: CD - Deploy Model

on:
  workflow_dispatch:
    inputs:
      model_version:
        description: "Model version to deploy"
        required: true

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production  # Requires approval
    steps:
      - name: Azure Login
        uses: azure/login@v2
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}

      - name: Deploy to Green (10% traffic)
        run: |
          az ml online-deployment create \
            --name green \
            --endpoint-name sales-forecast-endpoint \
            --model azureml:sales-forecast-model:${{ inputs.model_version }} \
            --instance-type Standard_DS2_v2 \
            --instance-count 1

          az ml online-endpoint update \
            --name sales-forecast-endpoint \
            --traffic "blue=90 green=10"

      - name: Smoke Test
        run: |
          python mlops/tests/smoke_test.py \
            --endpoint sales-forecast-endpoint \
            --deployment green

      - name: Promote Green to 100%
        run: |
          az ml online-endpoint update \
            --name sales-forecast-endpoint \
            --traffic "green=100"

          # Xóa blue deployment cũ
          az ml online-deployment delete \
            --name blue \
            --endpoint-name sales-forecast-endpoint \
            --yes
```

#### 3.2.4 Unit Tests cần viết

```
tests/
├── test_data_prep.py         # Test feature engineering
├── test_model_training.py    # Test model train pipeline
├── test_model_scoring.py     # Test endpoint input/output format
├── test_data_validation.py   # Test data schema + quality
└── test_webapp.py            # Test Flask routes
```

**Deliverables Phase 2:**
- [ ] GitHub Actions CI: lint + test + trigger training
- [ ] GitHub Actions CD: deploy model với blue/green
- [ ] Unit test coverage > 80%
- [ ] Environment protection rules (approval gate cho production)

---

### Phase 3: Monitoring & Data Drift (Tuần 5-6)

**Mục tiêu:** Giám sát model performance + phát hiện data drift + auto-retrain

#### 3.3.1 Monitoring Dashboard

```
┌─────────────────────────────────────────────────────┐
│              ML MONITORING DASHBOARD                │
│                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │ Accuracy │  │ Latency  │  │ Data Drift Score │  │
│  │  R²=0.92 │  │  45ms    │  │  0.12 (OK)       │  │
│  │  ✅ OK   │  │  ✅ OK   │  │  ✅ < 0.25       │  │
│  └──────────┘  └──────────┘  └──────────────────┘  │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ Prediction vs Actual (Last 7 days)          │    │
│  │  ───── Predicted   ──── Actual              │    │
│  │  📈 Chart line đối chiếu                    │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ Feature Distribution Shift                  │    │
│  │  hour:       ▓▓▓▓░░░░ 0.05 OK              │    │
│  │  quantity:   ▓▓▓▓▓░░░ 0.08 OK              │    │
│  │  base_price: ▓▓▓▓▓▓▓░ 0.22 ⚠️ WARNING      │    │
│  │  discount:   ▓▓▓▓▓▓▓▓ 0.31 🔴 DRIFT!       │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

#### 3.3.2 Data Drift Detection

```python
# mlops/monitoring/data_drift.py

from azure.ai.ml.entities import (
    DataDriftMonitor,
    DataDriftMetricThreshold,
    AlertNotification,
)

# Tạo monitor so sánh dữ liệu training vs dữ liệu production
monitor = DataDriftMonitor(
    name="sales-data-drift",
    baseline_data=Input(path="azureml:sales_training_data:latest"),
    target_data=Input(path="azureml:sales_production_data:latest"),
    compute="cpu-cluster",
    frequency="Day",
    features=["hour", "day_of_week", "month", "category_id",
              "region_id", "base_price", "discount_percent", "quantity"],
    metric_thresholds=[
        DataDriftMetricThreshold(
            metric="NormalizedWassersteinDistance",
            threshold=0.25,  # Trigger alert nếu drift > 0.25
        ),
        DataDriftMetricThreshold(
            metric="PopulationStabilityIndex",
            threshold=0.20,
        ),
    ],
    alert_notification=AlertNotification(
        emails=["team@company.com"],
    ),
)
```

#### 3.3.3 Model Performance Monitoring

```python
# mlops/monitoring/model_monitor.py

import pyodbc
from datetime import datetime, timedelta

def check_model_performance(conn, lookback_days=7):
    """So sánh predicted vs actual revenue hàng tuần."""
    query = """
        SELECT
            AVG(ABS(f.predicted_revenue - s.actual_revenue)) AS mae,
            AVG(ABS(f.predicted_revenue - s.actual_revenue)
                / NULLIF(s.actual_revenue, 0)) * 100 AS mape,
            COUNT(*) AS n_predictions
        FROM SalesForecast f
        JOIN (
            SELECT region, category,
                   CAST(event_timestamp AS DATE) AS forecast_date,
                   DATEPART(HOUR, event_timestamp) AS forecast_hour,
                   SUM(final_amount) AS actual_revenue
            FROM SalesTransactions
            WHERE event_timestamp >= DATEADD(DAY, -?, GETDATE())
            GROUP BY region, category,
                     CAST(event_timestamp AS DATE),
                     DATEPART(HOUR, event_timestamp)
        ) s ON f.region = s.region
            AND f.category = s.category
            AND f.forecast_date = s.forecast_date
            AND f.forecast_hour = s.forecast_hour
        WHERE f.created_at >= DATEADD(DAY, -?, GETDATE())
    """
    cursor = conn.cursor()
    cursor.execute(query, (lookback_days, lookback_days))
    row = cursor.fetchone()

    mae, mape, n = row
    status = "OK" if mape < 15 else ("WARNING" if mape < 25 else "CRITICAL")

    return {
        "mae": round(mae, 2) if mae else None,
        "mape_%": round(mape, 2) if mape else None,
        "n_predictions": n,
        "status": status,
        "should_retrain": mape is not None and mape > 20,
    }
```

#### 3.3.4 Auto-Retrain Trigger

```python
# mlops/monitoring/retrain_trigger.py

def evaluate_retrain_conditions():
    """Kiểm tra các điều kiện trigger retrain."""

    conditions = {
        "data_drift_detected": check_data_drift() > 0.25,
        "performance_degraded": check_model_performance()["mape_%"] > 20,
        "scheduled_weekly": is_sunday_3am(),
        "new_data_volume": get_new_records_count() > 10000,
    }

    should_retrain = any(conditions.values())

    if should_retrain:
        reason = [k for k, v in conditions.items() if v]
        trigger_training_pipeline(reason=reason)
        send_notification(f"Retrain triggered: {reason}")

    return {"should_retrain": should_retrain, "conditions": conditions}
```

**Deliverables Phase 3:**
- [ ] Data Drift Monitor chạy daily
- [ ] Model accuracy tracking (predicted vs actual)
- [ ] Auto-retrain khi drift > threshold hoặc MAPE > 20%
- [ ] Email/Teams alerts khi có vấn đề

---

### Phase 4: Infrastructure as Code & Governance (Tuần 7-8)

**Mục tiêu:** Toàn bộ infra dạng code, reproducible, có audit trail

#### 3.4.1 Terraform cho Azure Resources

```hcl
# mlops/infra/main.tf

resource "azurerm_machine_learning_workspace" "aml" {
  name                = "aml-sales-forecast"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku_name            = "Basic"

  identity { type = "SystemAssigned" }
}

resource "azurerm_machine_learning_compute_cluster" "training" {
  name                          = "training-cluster"
  machine_learning_workspace_id = azurerm_machine_learning_workspace.aml.id
  vm_size                       = "Standard_DS3_v2"
  vm_priority                   = "LowPriority"  # Tiết kiệm ~80% cost

  scale_settings {
    min_node_count                       = 0
    max_node_count                       = 4
    scale_down_nodes_after_idle_duration  = "PT5M"
  }
}

resource "azurerm_machine_learning_online_endpoint" "inference" {
  name                         = "sales-forecast-endpoint"
  machine_learning_workspace_id = azurerm_machine_learning_workspace.aml.id
  auth_mode                    = "key"
}
```

#### 3.4.2 Model Governance

```
┌──────────────────────────────────────────────────────┐
│                  MODEL REGISTRY                      │
│                                                      │
│  sales-forecast-model                                │
│  ├── v1  │ 2024-01-15 │ R²=0.85 │ ❌ Archived      │
│  ├── v2  │ 2024-02-01 │ R²=0.89 │ ❌ Archived      │
│  ├── v3  │ 2024-03-10 │ R²=0.92 │ ✅ Production    │
│  └── v4  │ 2024-03-25 │ R²=0.88 │ ⛔ Rejected      │
│                                                      │
│  Tags: team=data-science, use-case=demand-forecast   │
│  Properties: framework=sklearn, algo=GradientBoosting│
│                                                      │
│  Lineage: dataset v5 → pipeline run #47 → model v3  │
└──────────────────────────────────────────────────────┘
```

#### 3.4.3 Feature Store (tùy chọn nâng cao)

```python
# Centralize feature definitions để reuse giữa training và inference
feature_store = {
    "sales_features": {
        "hour_sin":      "sin(2π × hour / 24)",
        "hour_cos":      "cos(2π × hour / 24)",
        "month_sin":     "sin(2π × month / 12)",
        "month_cos":     "cos(2π × month / 12)",
        "is_weekend":    "1 if day_of_week >= 5 else 0",
        "price_x_qty":   "base_price × quantity",
        "net_price":     "base_price × (1 - discount_percent/100)",
    },
    "source": "Azure SQL → SalesTransactions",
    "refresh": "Hourly",
}
```

**Deliverables Phase 4:**
- [ ] Terraform scripts cho toàn bộ ML infrastructure
- [ ] Model versioning + lineage tracking
- [ ] RBAC: Data Scientist (train), ML Engineer (deploy), Admin (infra)
- [ ] Cost tagging + budget alerts cho ML resources

---

## 4. Cấu trúc thư mục MLOps

```
mlops/
├── pipeline/
│   ├── pipeline.py              # Pipeline definition
│   ├── pipeline_job.yml         # Pipeline job YAML
│   ├── data_prep.py             # Step 1: Data preparation
│   ├── train_step.py            # Step 2: Training + HyperDrive
│   ├── evaluate.py              # Step 3: Evaluate vs production
│   └── register.py              # Step 4: Register model
├── monitoring/
│   ├── data_drift.py            # Data drift detection
│   ├── model_monitor.py         # Performance monitoring
│   └── retrain_trigger.py       # Auto-retrain logic
├── tests/
│   ├── smoke_test.py            # Endpoint smoke test
│   ├── test_data_validation.py  # Data schema checks
│   └── test_model_quality.py    # Model quality gates
├── infra/
│   ├── main.tf                  # Terraform main
│   ├── variables.tf             # Terraform variables
│   └── outputs.tf               # Terraform outputs
├── .github/workflows/
│   ├── ci.yml                   # CI: test + validate
│   ├── cd-model.yml             # CD: deploy model
│   └── cd-webapp.yml            # CD: deploy web app
└── config/
    ├── dev.yml                  # Dev environment config
    ├── staging.yml              # Staging config
    └── prod.yml                 # Production config
```

---

## 5. Timeline tổng hợp

```
Tuần 1-2: Phase 1 - ML Pipeline Automation
  ├── Tuần 1: Tạo 4 pipeline steps + HyperDrive
  └── Tuần 2: Evaluation gate + weekly schedule

Tuần 3-4: Phase 2 - CI/CD Pipeline
  ├── Tuần 3: CI (lint, test, trigger) + unit tests
  └── Tuần 4: CD (blue/green deploy) + approval gate

Tuần 5-6: Phase 3 - Monitoring & Drift
  ├── Tuần 5: Data drift monitor + perf tracking
  └── Tuần 6: Auto-retrain trigger + alerting

Tuần 7-8: Phase 4 - IaC & Governance
  ├── Tuần 7: Terraform + model registry policies
  └── Tuần 8: RBAC + feature store + documentation
```

---

## 6. Công cụ & Dịch vụ cần thêm

| Công cụ | Vai trò | Chi phí ước tính |
|---------|---------|------------------|
| **GitHub Actions** | CI/CD pipeline | Free (2,000 min/tháng) |
| **Azure ML Pipeline** | Automated training | ~$5-20/pipeline run |
| **Azure ML Monitor** | Data drift detection | Included in AML |
| **Azure Container Registry** | Docker images | ~$5/tháng (Basic) |
| **Azure Key Vault** | Secrets management | ~$0.03/10K operations |
| **Terraform** | Infrastructure as Code | Free (open source) |
| **Azure Monitor + Log Analytics** | Observability | ~$2-5/tháng |
| **Tổng thêm** | | **~$15-35/tháng** |

---

## 7. Metrics đo lường thành công MLOps

| Metric | Trước MLOps | Sau MLOps | Mục tiêu |
|--------|-------------|-----------|---------|
| Thời gian từ data → model mới | 2-4 giờ (thủ công) | 30-60 phút (auto) | < 1 giờ |
| Thời gian deploy model | 1-2 giờ (thủ công) | 5-10 phút (CI/CD) | < 15 phút |
| Phát hiện model degradation | Không biết | < 24 giờ (monitor) | < 1 ngày |
| Rollback khi lỗi | 30-60 phút | 2 phút (blue/green) | < 5 phút |
| Test coverage | 0% | > 80% | > 80% |
| Reproducibility | Không | 100% (pipeline + IaC) | 100% |
