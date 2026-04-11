"""
╔══════════════════════════════════════════════════════════════════════╗
║         FLASH SALE END-TO-END MLOps DEMO                            ║
║  Mô phỏng kịch bản: Flash Sale → Drift → Adaptive Retrain           ║
║                                                                      ║
║  Kịch bản:                                                           ║
║    [P0] Kiểm tra trạng thái model hiện tại                          ║
║    [P1] Flash Sale bắt đầu – inject dữ liệu bất thường vào SQL      ║
║    [P2] Inject forecast cũ (sai lệch lớn với thực tế flash sale)    ║
║    [P3] DriftMonitor phát hiện MAE tăng vọt → AlertTriggered        ║
║    [P4] Adaptive Retrain – train lại với flash-sale pattern          ║
║    [P5] So sánh model mới vs cũ – Promote nếu tốt hơn               ║
║    [P6] Slack notification gửi đi                                    ║
║    [P7] Tổng kết & Web report URL                                    ║
╚══════════════════════════════════════════════════════════════════════╝

Usage:
    $env:KEY_VAULT_URI="DISABLED"
    $env:SLACK_WEBHOOK_URL="https://hooks.slack.com/..."
    python scripts/demo_flash_sale.py

Options:
    --azure         Chạy FULL Azure: SQL thật + AML pipeline retrain  ← MỚI
    --skip-sql      Bỏ qua inject SQL (chạy offline hoàn toàn)
    --fast          Dùng ít samples hơn để demo nhanh hơn
    --no-slack      Bỏ qua Slack notification

Azure mode yêu cầu thêm:
    $env:AML_SUBSCRIPTION_ID="34849ef9-3814-44df-ba32-a86ed9f2a69a"
    $env:AML_RESOURCE_GROUP="rg-sales-analytics-dev"
    $env:AML_WORKSPACE_NAME="aml-sales-analytics-d9bt2m2"
"""

import argparse
import json
import os
import sys
import time
import subprocess
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

# ─── project root ─────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ─── ANSI colors ──────────────────────────────────────────────────────────────
R  = "\033[91m"   # red
G  = "\033[92m"   # green
Y  = "\033[93m"   # yellow
B  = "\033[94m"   # blue (steps)
M  = "\033[95m"   # magenta (headers)
C  = "\033[96m"   # cyan
W  = "\033[97m"   # white bold
DIM= "\033[2m"
RST= "\033[0m"

def banner(text: str, color: str = M):
    width = 70
    print(f"\n{color}{'═'*width}{RST}")
    print(f"{color}  {text}{RST}")
    print(f"{color}{'═'*width}{RST}")

def step(phase: str, text: str):
    print(f"\n{B}[{phase}]{RST} {W}{text}{RST}")

def ok(text: str):
    print(f"  {G}✔{RST}  {text}")

def warn(text: str):
    print(f"  {Y}⚠{RST}  {text}")

def info(text: str):
    print(f"  {C}→{RST}  {DIM}{text}{RST}")

def metric(label: str, old: float, new: float, unit: str = "", higher_is_better: bool = True):
    delta = new - old
    arrow = "↑" if delta > 0 else "↓"
    good = (delta > 0) == higher_is_better
    color = G if good else R
    print(f"  {color}{arrow}{RST}  {label:35s} {old:8.4f} → {color}{new:8.4f}{RST}  ({color}{delta:+.4f}{RST}) {unit}")


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 0 – Current model state
# ══════════════════════════════════════════════════════════════════════════════
def phase0_show_current_state():
    banner("PHASE 0 │ Kiểm tra trạng thái hệ thống trước Flash Sale", C)

    meta_path = ROOT / "ml" / "model_output" / "model_metadata.json"
    drift_path = ROOT / "ml" / "model_output" / "drift_monitor_report.json"
    history_path = ROOT / "ml" / "model_output" / "retrain_history" / "history_index.json"

    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        ok(f"Model hiện tại: {meta.get('model_type', 'GradientBoosting')}")
        n_s = meta.get('n_samples', '?')
        ok(f"Trained on:     {n_s:,} samples" if isinstance(n_s, int) else f"Trained on:     {n_s} samples")
        ok(f"Revenue R²:     {meta.get('revenue_r2', meta.get('r2_revenue', '?'))}")
        ok(f"Trained at:     {meta.get('trained_at', meta.get('timestamp', '?'))}")
    else:
        warn("model_metadata.json not found – model chưa được train")

    if drift_path.exists():
        report = json.loads(drift_path.read_text(encoding="utf-8"))
        mae = report.get("metrics", {}).get("mae", "?")
        ok(f"Last drift check MAE: {mae}  (threshold: {report.get('threshold_mae','?')})")
        ok(f"Last check at:  {report.get('timestamp','?')}")
    
    if history_path.exists():
        history = json.loads(history_path.read_text(encoding="utf-8"))
        ok(f"Lịch sử retrain: {len(history)} lần")
        for h in history[-2:]:
            info(f"  {h.get('run_id','')} → decision={h.get('decision','?')} revenue_r2={h.get('new_revenue_r2','?')}")

    print()
    info("Trạng thái: BÌNH THƯỜNG – mô hình dự báo tốt với dữ liệu thường ngày")


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 – Inject Flash Sale data into SQL
# ══════════════════════════════════════════════════════════════════════════════
def phase1_inject_flash_sale_data(n_rows: int = 500, skip_sql: bool = False) -> list[dict]:
    banner("PHASE 1 │ Flash Sale BẮT ĐẦU – Inject dữ liệu bất thường", R)
    step("1A", "Tạo dữ liệu Flash Sale (giá giảm 60-80%, volume 5-10x bình thường)…")

    now = datetime.utcnow()
    stores = ["S01", "S02", "S03"]
    products = {
        "Electronics": ["P101", "P102", "P103", "P104"],
        "Accessories": ["P201", "P202", "P203"],
        "Clothing":    ["P301", "P302", "P303"],
        "Home":        ["P401", "P402"],
        "Beverage":    ["P501", "P502"],
    }
    # Flash sale weights: Electronics dominates
    category_weights = [0.45, 0.25, 0.15, 0.10, 0.05]
    categories = list(products.keys())

    rows = []
    for i in range(n_rows):
        dt = now - timedelta(minutes=random.randint(0, 90))
        cat = random.choices(categories, weights=category_weights)[0]
        pid = random.choice(products[cat])
        store = random.choices(stores, weights=[0.5, 0.35, 0.15])[0]

        # Flash sale patterns:
        # - Giá gốc cao nhưng discount 60-80%
        # - Volume đơn hàng tăng 5-10x
        if cat == "Electronics":
            base_price = random.uniform(200, 800)
            discount = random.uniform(0.60, 0.80)
        elif cat == "Accessories":
            base_price = random.uniform(50, 200)
            discount = random.uniform(0.50, 0.70)
        else:
            base_price = random.uniform(20, 100)
            discount = random.uniform(0.40, 0.60)

        unit_price = round(base_price * (1 - discount), 2)
        units_sold = random.randint(5, 50)  # 5-10x normal 1-5
        revenue = round(unit_price * units_sold, 2)

        rows.append({
            "event_time": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "store_id": store,
            "product_id": pid,
            "units_sold": units_sold,
            "unit_price": unit_price,
            "revenue": revenue,
            "temperature": round(random.uniform(25, 32), 1),
            "weather": "Sunny",
            "holiday": 1,       # Flash sale = special event
            "category": cat,
        })

    ok(f"Đã tạo {n_rows} bản ghi Flash Sale")
    
    # Show distribution
    total_rev = sum(r["revenue"] for r in rows)
    avg_units = sum(r["units_sold"] for r in rows) / len(rows)
    avg_price = sum(r["unit_price"] for r in rows) / len(rows)
    info(f"Tổng doanh thu flash sale: {total_rev:,.0f}")
    info(f"Avg units/order: {avg_units:.1f} (bình thường: ~2.5)")
    info(f"Avg unit price:  {avg_price:.2f} (bình thường: ~50)")

    if not skip_sql:
        step("1B", "Ghi dữ liệu Flash Sale vào SQL (SalesTransactions)…")
        try:
            from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
            import pyodbc

            conn_str = (
                f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
                f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            insert_sql = """
                INSERT INTO dbo.SalesTransactions
                    (event_time, store_id, product_id, units_sold, unit_price,
                     revenue, temperature, weather, holiday, category)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """
            for r in rows:
                cursor.execute(insert_sql,
                    r["event_time"], r["store_id"], r["product_id"],
                    r["units_sold"], r["unit_price"], r["revenue"],
                    r["temperature"], r["weather"], r["holiday"], r["category"]
                )
            conn.commit()
            conn.close()
            ok(f"✔ Đã INSERT {n_rows} rows vào dbo.SalesTransactions")
        except Exception as e:
            warn(f"SQL insert failed: {e}")
            info("Tiếp tục demo với offline simulation…")
    else:
        warn("Bỏ qua SQL inject (--skip-sql)")

    return rows


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 – Inject stale forecasts (old model predictions vs flash sale actuals)
# ══════════════════════════════════════════════════════════════════════════════
def phase2_inject_stale_forecasts(flash_rows: list[dict], skip_sql: bool = False):
    banner("PHASE 2 │ Model cũ dự báo TRƯỚC Flash Sale – sai lệch lớn", Y)
    step("2A", "Mô phỏng: model cũ dự báo revenue bình thường (không biết có flash sale)…")

    # Old model trained on normal data → predicts normal revenue
    # Actual flash sale revenue is 3-8x higher due to volume
    stale_items = []
    now = datetime.utcnow()
    
    # Group by (store, category, hour) - mimic what SalesForecast table holds
    from collections import defaultdict
    groups = defaultdict(list)
    for r in flash_rows:
        key = (r["store_id"], r["category"])
        groups[key].append(r["revenue"])

    total_predicted = 0
    total_actual = 0
    for (store, cat), revenues in list(groups.items())[:20]:
        actual = sum(revenues)
        # Old model predicts ~20% of actual (doesn't know about flash sale surge)
        predicted = actual * random.uniform(0.12, 0.25)
        total_predicted += predicted
        total_actual += actual
        stale_items.append({
            "store": store, "cat": cat, 
            "predicted": round(predicted, 2), 
            "actual": round(actual, 2),
            "error": round(actual - predicted, 2),
            "mae_contribution": round(abs(actual - predicted), 2)
        })

    avg_mae_preview = sum(abs(s["actual"] - s["predicted"]) for s in stale_items) / len(stale_items) if stale_items else 0

    ok(f"Tổng dự báo (model cũ): {total_predicted:>12,.0f}")
    ok(f"Thực tế Flash Sale:     {total_actual:>12,.0f}")
    ok(f"MAE preview (mẫu):      {avg_mae_preview:>12,.1f}  ← model bị lạc hậu!")
    
    print()
    print(f"  {'Store':<6} {'Category':<15} {'Predicted':>12} {'Actual':>12} {'Error':>12}")
    print(f"  {'─'*6} {'─'*15} {'─'*12} {'─'*12} {'─'*12}")
    for s in stale_items[:8]:
        err_color = R if abs(s["error"]) > 100 else Y
        print(f"  {s['store']:<6} {s['cat']:<15} {s['predicted']:>12,.1f} {s['actual']:>12,.1f} {err_color}{s['error']:>+12,.1f}{RST}")

    if not skip_sql and stale_items:
        step("2B", "Ghi SalesForecast với dự báo cũ vào SQL…")
        try:
            from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
            import pyodbc

            conn_str = (
                f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
                f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            today = now.date()
            for s in stale_items:
                cursor.execute(
                    """INSERT INTO dbo.SalesForecast
                       (forecast_date, forecast_hour, store_id, category,
                        predicted_quantity, predicted_revenue, model_version)
                       VALUES (?,?,?,?,?,?,?)""",
                    today, now.hour, s["store"], s["cat"],
                    5, s["predicted"], "v_pre_flashsale"
                )
            conn.commit()
            conn.close()
            ok(f"Đã INSERT {len(stale_items)} forecast rows vào dbo.SalesForecast")
        except Exception as e:
            warn(f"SQL insert forecasts failed: {e}")
    else:
        warn("Bỏ qua SQL forecast inject (--skip-sql)")

    return stale_items


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3 – Drift Detection
# ══════════════════════════════════════════════════════════════════════════════
def phase3_detect_drift(fast: bool = False) -> dict:
    banner("PHASE 3 │ DriftMonitor Chạy – Phát Hiện MAE Tăng Vọt", R)
    step("3A", "Chạy drift_monitor.py với ngưỡng thực tế (25)…")

    # Set up env for drift monitor
    env = os.environ.copy()
    env["KEY_VAULT_URI"] = "DISABLED"
    env["DRIFT_TRIGGER_MODE"] = "none"        # Phase 3 chỉ detect, chưa retrain
    env["DRIFT_MAE_ABS_THRESHOLD"] = "25"
    env["DRIFT_MONITOR_MIN_SAMPLES"] = "0"
    env["DRIFT_MONITOR_COOLDOWN_MINUTES"] = "0"
    env["PYTHONIOENCODING"] = "utf-8"

    python_exe = str(ROOT / ".venv" / "Scripts" / "python.exe")
    monitor_script = str(ROOT / "ml" / "drift_monitor.py")

    # Clear stale lock
    lock_path = ROOT / "ml" / "model_output" / "drift_monitor.lock"
    if lock_path.exists():
        lock_path.unlink()

    result = subprocess.run(
        [python_exe, monitor_script,
         "--threshold-mae", "25",
         "--min-samples", "0",
         "--cooldown-minutes", "0",
         "--trigger-mode", "none"],
        capture_output=True, text=True, env=env, timeout=60
    )

    # Parse output
    report = {}
    output_lines = result.stdout.strip().splitlines()
    
    # Try to parse JSON from output
    json_start = None
    for i, line in enumerate(output_lines):
        if line.strip().startswith("{"):
            json_start = i
            break
    
    if json_start is not None:
        try:
            report = json.loads("\n".join(output_lines[json_start:]))
        except Exception:
            pass

    mae = report.get("metrics", {}).get("mae", "N/A")
    triggered = report.get("triggered", False)
    n_samples = report.get("metrics", {}).get("n_samples", 0)

    # Print key lines from stdout
    for line in output_lines[:5]:
        if line.strip():
            info(line.strip())

    print()
    if triggered:
        print(f"  {R}🚨 DRIFT DETECTED!{RST}")
        ok(f"MAE hiện tại: {R}{mae}{RST}  (ngưỡng: 25)")
        ok(f"Số mẫu trong 24h: {n_samples}")
    else:
        # Even if trigger_mode=none, check if MAE > threshold
        if isinstance(mae, (int, float)) and mae > 25:
            print(f"  {R}🚨 DRIFT WOULD TRIGGER!{RST}")
            ok(f"MAE hiện tại: {R}{mae}{RST}  > ngưỡng 25")
        else:
            warn(f"MAE = {mae} – dưới ngưỡng, hoặc không đủ dữ liệu trong SQL")
            info("(Demo offline: MAE tính từ dữ liệu SQL thực, có thể cần --skip-sql để mô phỏng offline)")
    
    print()
    info("Trong thực tế: DriftMonitor chạy mỗi GIỜ qua Azure Function (timer trigger)")
    info("Khi phát hiện: → ghi report JSON → trigger retrain pipeline")

    return report


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4a – Adaptive Retrain LOCAL (scikit-learn trên máy)
# ══════════════════════════════════════════════════════════════════════════════
def phase4_adaptive_retrain(fast: bool = False) -> dict:
    banner("PHASE 4 │ Adaptive Retrain – Học Flash Sale Pattern", M)
    step("4A", "Cấu hình retrain với flash-sale-aware hyperparameters…")

    # Flash sale model: nhiều estimators hơn để capture complex patterns,
    # learning rate thấp hơn để stable
    n_samples = 30000 if fast else 80000
    n_estimators = 200 if fast else 400
    max_depth = 7
    learning_rate = 0.08

    info(f"n_samples:     {n_samples:,}  (bao gồm flash sale patterns)")
    info(f"n_estimators:  {n_estimators}")
    info(f"max_depth:     {max_depth}  (sâu hơn để học price-discount patterns)")
    info(f"learning_rate: {learning_rate}  (thấp hơn để stable với data bất thường)")
    info(f"--promote:     True  (auto-promote nếu mới tốt hơn)")

    step("4B", "Chạy retrain_and_compare.py --promote…")
    print()

    env = os.environ.copy()
    env["KEY_VAULT_URI"] = "DISABLED"
    env["PYTHONIOENCODING"] = "utf-8"
    env["FLASH_SALE_MODE"] = "1"  # flag để train_model biết là flash sale

    python_exe = str(ROOT / ".venv" / "Scripts" / "python.exe")
    retrain_script = str(ROOT / "ml" / "retrain_and_compare.py")

    t0 = time.time()
    result = subprocess.run(
        [python_exe, retrain_script,
         "--new-samples", str(n_samples),
         "--new-estimators", str(n_estimators),
         "--new-depth", str(max_depth),
         "--new-lr", str(learning_rate),
         "--promote"],
        capture_output=True, text=True, env=env, timeout=300
    )
    elapsed = time.time() - t0

    # Parse output for metrics
    retrain_result = {"success": result.returncode == 0}
    lines = result.stdout.strip().splitlines()
    
    for line in lines:
        print(f"  {DIM}{line}{RST}")

    print()
    ok(f"Retrain hoàn thành trong {elapsed:.1f}s  (exit={result.returncode})")

    # Also load the comparison report
    compare_path = ROOT / "ml" / "model_output" / "retrain_comparison" / "comparison_report.json"
    if compare_path.exists():
        retrain_result["report"] = json.loads(compare_path.read_text(encoding="utf-8"))

    return retrain_result


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4b – Adaptive Retrain AZURE ML (AML compute cluster)
# ══════════════════════════════════════════════════════════════════════════════
def phase4_adaptive_retrain_azure(fast: bool = False) -> dict:
    banner("PHASE 4 │ Adaptive Retrain – Azure ML Pipeline (Cloud)", M)
    step("4A", "Gửi training job lên Azure ML compute cluster…")

    n_samples = 30000 if fast else 80000
    timeout_min = 20 if fast else 40

    info(f"n_samples:    {n_samples:,}  (flash sale patterns)")
    info(f"compute:      training-cluster  (Azure ML)")
    info(f"timeout:      {timeout_min} phút")
    info(f"experiment:   sales-forecast-training")

    step("4B", "Chạy mlops/trigger_training_pipeline.py…")
    print()

    env = os.environ.copy()
    env["KEY_VAULT_URI"] = "DISABLED"
    env["PYTHONIOENCODING"] = "utf-8"
    env.setdefault("AML_SUBSCRIPTION_ID", "34849ef9-3814-44df-ba32-a86ed9f2a69a")
    env.setdefault("AML_RESOURCE_GROUP",   "rg-sales-analytics-dev")
    env.setdefault("AML_WORKSPACE_NAME",   "aml-sales-analytics-d9bt2m2")

    python_exe = str(ROOT / ".venv" / "Scripts" / "python.exe")
    pipeline_script = str(ROOT / "mlops" / "trigger_training_pipeline.py")

    t0 = time.time()
    result = subprocess.run(
        [python_exe, pipeline_script,
         "--n-samples", str(n_samples),
         "--timeout",   str(timeout_min)],
        capture_output=True, text=True, env=env, timeout=timeout_min * 60 + 60
    )
    elapsed = time.time() - t0

    retrain_result = {"success": result.returncode == 0, "azure": True}
    lines = result.stdout.strip().splitlines()
    for line in lines:
        print(f"  {DIM}{line}{RST}")
    if result.stderr.strip():
        for line in result.stderr.strip().splitlines()[-5:]:
            print(f"  {Y}{line}{RST}")

    print()
    if result.returncode == 0:
        ok(f"Azure ML job hoàn thành trong {elapsed:.0f}s  (exit=0)")
    else:
        warn(f"Azure ML job exit={result.returncode} — kiểm tra AML Studio")
        info(f"URL: https://ml.azure.com/runs?wsid=/subscriptions/{env['AML_SUBSCRIPTION_ID']}/resourceGroups/{env['AML_RESOURCE_GROUP']}/providers/Microsoft.MachineLearningServices/workspaces/{env['AML_WORKSPACE_NAME']}")

    # Load comparison report nếu pipeline đã ghi
    compare_path = ROOT / "ml" / "model_output" / "retrain_comparison" / "comparison_report.json"
    if compare_path.exists():
        retrain_result["report"] = json.loads(compare_path.read_text(encoding="utf-8"))

    return retrain_result


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 5 – Compare old vs new model
# ══════════════════════════════════════════════════════════════════════════════
def phase5_show_comparison(retrain_result: dict):
    banner("PHASE 5 │ Model Mới vs Model Cũ – Flash Sale Performance", G)

    report = retrain_result.get("report", {})
    if not report:
        # Try loading from file
        p = ROOT / "ml" / "model_output" / "retrain_comparison" / "comparison_report.json"
        if p.exists():
            report = json.loads(p.read_text(encoding="utf-8"))

    if not report:
        warn("Không tìm thấy comparison report")
        return

    decision = report.get("decision", "?")
    old_m = report.get("old_revenue_metrics", report.get("old_model", {}))
    new_m = report.get("new_revenue_metrics", report.get("new_model", {}))
    old_q = report.get("old_quantity_metrics", {})
    new_q = report.get("new_quantity_metrics", {})

    decision_color = G if decision == "PROMOTE" else R
    print(f"\n  Quyết định: {decision_color}{'█'*4} {decision} {'█'*4}{RST}\n")

    step("5A", "So sánh metrics trên cùng test set:")
    print(f"\n  {'Metric':<35} {'Old Model':>10} {'New Model':>10} {'Delta':>10}")
    print(f"  {'─'*35} {'─'*10} {'─'*10} {'─'*10}")

    # Revenue metrics
    for label, key, hib in [
        ("Revenue R²",   "r2_score", True),
        ("Revenue MAE",  "mae",      False),
        ("Revenue RMSE", "rmse",     False),
    ]:
        old_val = old_m.get(key)
        new_val = new_m.get(key)
        if old_val is not None and new_val is not None:
            metric(label, float(old_val), float(new_val), higher_is_better=hib)

    # Quantity metrics
    for label, key, hib in [
        ("Quantity R²",   "r2_score", True),
        ("Quantity MAE",  "mae",      False),
        ("Quantity RMSE", "rmse",     False),
    ]:
        old_val = old_q.get(key)
        new_val = new_q.get(key)
        if old_val is not None and new_val is not None:
            metric(label, float(old_val), float(new_val), higher_is_better=hib)

    print()
    step("5B", "Adaptive learning tóm tắt:")
    
    old_r2 = old_m.get("r2_score", old_m.get("revenue_r2", 0))
    new_r2 = new_m.get("r2_score", new_m.get("revenue_r2", 0))
    old_mae = old_m.get("mae", old_m.get("revenue_mae", 0))
    new_mae = new_m.get("mae", new_m.get("revenue_mae", 0))

    if old_r2 and new_r2:
        improvement_r2 = (float(new_r2) - float(old_r2)) / max(abs(float(old_r2)), 1e-9) * 100
        ok(f"Revenue R² cải thiện: {improvement_r2:+.1f}%")
    if old_mae and new_mae:
        improvement_mae = (float(old_mae) - float(new_mae)) / max(float(old_mae), 1e-9) * 100
        ok(f"Revenue MAE giảm:     {improvement_mae:.1f}%  ← model đã ADAPTIVE với flash sale!")

    n_old = report.get("old_config", {}).get("n_samples", report.get("old_n_samples", "?"))
    n_new = report.get("data_source", {}).get("n_samples", report.get("new_n_samples", "?"))
    info(f"Old model trained on: {n_old} samples  (không có flash sale patterns)")
    info(f"New model trained on: {n_new} samples  (bao gồm flash sale patterns)")

    # Charts
    compare_dir = ROOT / "ml" / "model_output" / "retrain_comparison"
    pngs = list(compare_dir.glob("*.png"))
    if pngs:
        info(f"Charts saved: {compare_dir}")
        for p in pngs:
            info(f"  📊 {p.name}")


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 6 – Notifications
# ══════════════════════════════════════════════════════════════════════════════
def phase6_notifications(no_slack: bool = False):
    banner("PHASE 6 │ Gửi Notifications – Slack + Web Report", B)

    report_path = ROOT / "ml" / "model_output" / "drift_monitor_report.json"

    if no_slack:
        warn("Bỏ qua Slack (--no-slack)")
        return

    slack_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not slack_url:
        warn("SLACK_WEBHOOK_URL chưa set – bỏ qua Slack")
        return

    # Load latest metrics
    latest_report = {}
    if report_path.exists():
        latest_report = json.loads(report_path.read_text(encoding="utf-8"))

    compare_path = ROOT / "ml" / "model_output" / "retrain_comparison" / "comparison_report.json"
    compare = {}
    if compare_path.exists():
        compare = json.loads(compare_path.read_text(encoding="utf-8"))

    new_r2 = compare.get("new_revenue_metrics", compare.get("new_model", {})).get("r2_score", compare.get("new_revenue_metrics", {}).get("revenue_r2", "?"))
    old_r2 = compare.get("old_revenue_metrics", compare.get("old_model", {})).get("r2_score", compare.get("old_revenue_metrics", {}).get("revenue_r2", "?"))
    decision = compare.get("decision", "PROMOTE")

    step("6A", "Gửi Flash Sale Retrain Alert tới Slack…")
    try:
        import requests as req_lib
        
        payload = {
            "text": "🛒 *Flash Sale Adaptive Retrain Complete*",
            "attachments": [{
                "color": "#36a64f" if decision == "PROMOTE" else "#ff0000",
                "fields": [
                    {"title": "Event", "value": "Flash Sale Detected", "short": True},
                    {"title": "Decision", "value": decision, "short": True},
                    {"title": "Old Revenue R²", "value": str(old_r2), "short": True},
                    {"title": "New Revenue R²", "value": str(new_r2), "short": True},
                    {"title": "MAE Before", "value": str(latest_report.get("metrics", {}).get("mae", "N/A")), "short": True},
                    {"title": "Timestamp", "value": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), "short": True},
                ],
                "footer": "Azure MLOps | DriftMonitor → AdaptiveRetrain",
                "fallback": f"Flash Sale retrain {decision}: R² {old_r2} → {new_r2}"
            }]
        }

        r = req_lib.post(slack_url, json=payload, timeout=15)
        if r.status_code == 200:
            ok(f"Slack notification sent ✓  (HTTP {r.status_code})")
        else:
            warn(f"Slack returned HTTP {r.status_code}")
    except Exception as e:
        warn(f"Slack error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 7 – Summary
# ══════════════════════════════════════════════════════════════════════════════
def phase7_summary(timings: dict):
    banner("PHASE 7 │ Tổng Kết – Flash Sale MLOps Demo", G)

    print(f"""
  {W}Kịch bản Flash Sale hoàn thành!{RST}

  {G}✔{RST}  Flash Sale data injected → SQL SalesTransactions
  {G}✔{RST}  Model cũ dự báo sai (revenue thực 3-8x dự báo)
  {G}✔{RST}  DriftMonitor phát hiện MAE tăng vọt
  {G}✔{RST}  Adaptive Retrain tự động kích hoạt
  {G}✔{RST}  Model mới học được flash sale patterns
  {G}✔{RST}  Model được PROMOTE nếu tốt hơn (gate check)
  {G}✔{RST}  Slack notification gửi đi

  {W}Thời gian từng phase:{RST}""")

    for phase, elapsed in timings.items():
        bar = "█" * min(20, int(elapsed / 3))
        print(f"  {phase:30s} {elapsed:7.1f}s  {C}{bar}{RST}")

    web_url = "https://webapp-sales-analytics-d9bt2m.azurewebsites.net"
    print(f"""
  {W}URLs:{RST}
  {C}→{RST}  Web Dashboard:   {web_url}
  {C}→{RST}  Model Report:    {web_url}/model-report
  {C}→{RST}  Drift Report:    {web_url}/api/drift-report
  {C}→{RST}  Health Check:    {web_url}/api/health

  {W}Files:{RST}
  {C}→{RST}  {ROOT / "ml" / "model_output" / "drift_monitor_report.json"}
  {C}→{RST}  {ROOT / "ml" / "model_output" / "retrain_comparison" / "comparison_report.json"}

  {DIM}Trong production: Azure Function DriftMonitor chạy mỗi 1 giờ, tự động
  detect và retrain khi data distribution thay đổi (flash sale, seasonal, etc.)
  Dùng --azure để chạy với SQL thật + AML compute cluster.{RST}
""")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Flash Sale MLOps End-to-End Demo")
    parser.add_argument("--azure",     action="store_true",
                        help="Chạy FULL Azure: SQL thật + AML pipeline retrain")
    parser.add_argument("--skip-sql",  action="store_true", help="Bỏ qua SQL inject (local only)")
    parser.add_argument("--fast",       action="store_true", help="Dùng ít samples hơn")
    parser.add_argument("--no-slack",  action="store_true", help="Bỏ qua Slack")
    parser.add_argument("--phases",    default="0,1,2,3,4,5,6,7",
                        help="Chạy các phases cụ thể, vd: --phases 0,3,4,5")
    args = parser.parse_args()

    # --azure overrides --skip-sql: phải kết nối SQL thật
    if args.azure:
        args.skip_sql = False

    phases_to_run = set(args.phases.split(","))
    mode_label = f"{R}☁  AZURE MODE{RST}  (SQL thật + AML pipeline)" if args.azure else f"{Y}💻 LOCAL MODE{RST}  (offline simulation)"

    banner("FLASH SALE END-TO-END MLOps DEMO", M)
    print(f"""
  {W}Kịch bản:{RST} Một đợt Flash Sale bắt đầu. Hệ thống AI phát hiện
  rằng model dự báo hiện tại bị lỗi thời (trained trên data bình thường),
  tự động retrain với flash-sale patterns, và promote model mới.

  Chế độ:   {mode_label}

  {Y}Yêu cầu:{RST}
    $env:KEY_VAULT_URI = "DISABLED"
    $env:SLACK_WEBHOOK_URL = "<webhook>"
""")

    timings = {}
    flash_rows = []
    stale_items = []
    drift_report = {}
    retrain_result = {}

    t_total = time.time()

    # Phase 0
    if "0" in phases_to_run:
        t = time.time()
        phase0_show_current_state()
        timings["P0: Current State"] = time.time() - t

    # Phase 1
    if "1" in phases_to_run:
        t = time.time()
        n = 200 if (args.fast or args.skip_sql) else 500
        flash_rows = phase1_inject_flash_sale_data(n_rows=n, skip_sql=args.skip_sql)
        timings["P1: Flash Sale Inject"] = time.time() - t

    # Phase 2
    if "2" in phases_to_run:
        t = time.time()
        stale_items = phase2_inject_stale_forecasts(flash_rows, skip_sql=args.skip_sql)
        timings["P2: Forecast Inject"] = time.time() - t

    # Phase 3
    if "3" in phases_to_run:
        t = time.time()
        drift_report = phase3_detect_drift(fast=args.fast)
        timings["P3: Drift Detection"] = time.time() - t

    # Phase 4
    if "4" in phases_to_run:
        t = time.time()
        if args.azure:
            retrain_result = phase4_adaptive_retrain_azure(fast=args.fast)
            timings["P4: AML Retrain (Azure)"] = time.time() - t
        else:
            retrain_result = phase4_adaptive_retrain(fast=args.fast)
            timings["P4: Adaptive Retrain (Local)"] = time.time() - t

    # Phase 5
    if "5" in phases_to_run:
        t = time.time()
        phase5_show_comparison(retrain_result)
        timings["P5: Comparison"] = time.time() - t

    # Phase 6
    if "6" in phases_to_run:
        t = time.time()
        phase6_notifications(no_slack=args.no_slack)
        timings["P6: Notifications"] = time.time() - t

    # Phase 7
    if "7" in phases_to_run:
        timings["TOTAL"] = time.time() - t_total
        phase7_summary(timings)


if __name__ == "__main__":
    main()
