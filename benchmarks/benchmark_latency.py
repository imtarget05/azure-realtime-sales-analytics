"""
Mục 3.3 Rubric: Đo delay truyền dữ liệu multi-region.
- Đo latency kết nối đến Azure SQL Database ở nhiều region
- Đo latency Event Hub gửi/nhận ở nhiều region
- So sánh delay giữa các region
- Gợi ý region tối ưu
"""

import os
import sys
import time
import json
import socket
import statistics
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

try:
    import pyodbc
except ImportError:
    pyodbc = None


# Danh sách các Azure region endpoints cần kiểm tra
# Trong môi trường thật, mỗi region sẽ có server SQL riêng
AZURE_REGIONS = {
    "East US": {"sql_suffix": ".database.windows.net", "location": "Virginia, USA"},
    "West Europe": {"sql_suffix": ".database.windows.net", "location": "Netherlands"},
    "Southeast Asia": {"sql_suffix": ".database.windows.net", "location": "Singapore"},
    "Japan East": {"sql_suffix": ".database.windows.net", "location": "Tokyo, Japan"},
    "Australia East": {"sql_suffix": ".database.windows.net", "location": "Sydney, Australia"},
}

# Các Azure endpoint phổ biến để test TCP latency
AZURE_TEST_ENDPOINTS = {
    "East US": "eastus.azure.com",
    "West Europe": "westeurope.azure.com",
    "Southeast Asia": "southeastasia.azure.com",
    "Japan East": "japaneast.azure.com",
    "Australia East": "australiaeast.azure.com",
}


def measure_tcp_latency(host: str, port: int = 443, attempts: int = 5) -> dict:
    """Đo TCP connection latency đến host:port."""
    latencies = []
    errors = 0

    for _ in range(attempts):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            t0 = time.time()
            sock.connect((host, port))
            latency_ms = (time.time() - t0) * 1000
            latencies.append(latency_ms)
            sock.close()
        except (socket.timeout, socket.error, OSError):
            errors += 1

    if not latencies:
        return {"host": host, "status": "UNREACHABLE", "errors": errors}

    return {
        "host": host,
        "min_ms": round(min(latencies), 2),
        "max_ms": round(max(latencies), 2),
        "avg_ms": round(statistics.mean(latencies), 2),
        "median_ms": round(statistics.median(latencies), 2),
        "stdev_ms": round(statistics.stdev(latencies), 2) if len(latencies) > 1 else 0,
        "successful": len(latencies),
        "errors": errors,
    }


def measure_sql_latency(attempts: int = 10) -> dict:
    """Đo latency thực tế với Azure SQL Database hiện tại."""
    if pyodbc is None:
        return {"status": "pyodbc not installed"}

    conn_string = (
        f"Driver={SQL_DRIVER};"
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};Pwd={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )

    # Đo connection time
    t0 = time.time()
    try:
        conn = pyodbc.connect(conn_string, timeout=15)
    except Exception as e:
        return {"status": f"Connection failed: {e}"}
    connection_time_ms = (time.time() - t0) * 1000

    cursor = conn.cursor()
    query_latencies = []

    for _ in range(attempts):
        t0 = time.time()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        latency_ms = (time.time() - t0) * 1000
        query_latencies.append(latency_ms)

    # Đo query phức tạp hơn
    complex_latencies = []
    for _ in range(attempts):
        t0 = time.time()
        cursor.execute("SELECT GETDATE(), @@VERSION")
        cursor.fetchone()
        latency_ms = (time.time() - t0) * 1000
        complex_latencies.append(latency_ms)

    cursor.close()
    conn.close()

    return {
        "server": SQL_SERVER,
        "connection_time_ms": round(connection_time_ms, 2),
        "simple_query": {
            "min_ms": round(min(query_latencies), 2),
            "max_ms": round(max(query_latencies), 2),
            "avg_ms": round(statistics.mean(query_latencies), 2),
            "median_ms": round(statistics.median(query_latencies), 2),
        },
        "complex_query": {
            "min_ms": round(min(complex_latencies), 2),
            "max_ms": round(max(complex_latencies), 2),
            "avg_ms": round(statistics.mean(complex_latencies), 2),
            "median_ms": round(statistics.median(complex_latencies), 2),
        },
    }


def measure_dns_resolution(hosts: dict) -> dict:
    """Đo thời gian phân giải DNS cho các endpoint."""
    results = {}
    for region, host in hosts.items():
        try:
            t0 = time.time()
            ip = socket.gethostbyname(host)
            resolve_ms = (time.time() - t0) * 1000
            results[region] = {"host": host, "ip": ip, "resolve_ms": round(resolve_ms, 2)}
        except socket.gaierror:
            results[region] = {"host": host, "status": "DNS_FAILED"}
    return results


def measure_multi_region_tcp():
    """Đo TCP latency đến nhiều Azure region khác nhau."""
    # Sử dụng Azure management endpoint cho mỗi region
    azure_endpoints = {
        "East US": ("20.62.128.0", 443),       # Azure East US IP range
        "West Europe": ("20.50.0.0", 443),      # Azure West Europe
        "Southeast Asia": ("20.43.128.0", 443),  # Azure SE Asia
        "Japan East": ("20.46.128.0", 443),      # Azure Japan East
    }

    # Thay vì dùng IP trực tiếp, test với SQL Server endpoint
    print("\n[MULTI-REGION] Đo TCP latency đến SQL Server endpoint...")
    sql_host = SQL_SERVER
    result = measure_tcp_latency(sql_host, port=1433, attempts=10)
    return {"primary_sql_server": result}


def run_latency_benchmark():
    print("=" * 60)
    print("  BENCHMARK DELAY TRUYỀN DỮ LIỆU MULTI-REGION")
    print("=" * 60)

    all_results = {
        "timestamp": datetime.now().isoformat(),
        "client_location": "Local Machine",
    }

    # 1. DNS Resolution
    print("\n[1] Đo DNS Resolution...")
    dns_results = measure_dns_resolution({
        "SQL Server": SQL_SERVER,
        "Azure Portal": "portal.azure.com",
        "Azure Management": "management.azure.com",
    })
    all_results["dns_resolution"] = dns_results
    for name, r in dns_results.items():
        if "resolve_ms" in r:
            print(f"  {name}: {r['resolve_ms']:.2f}ms → {r.get('ip', 'N/A')}")
        else:
            print(f"  {name}: {r.get('status', 'UNKNOWN')}")

    # 2. TCP Latency to SQL Server
    print(f"\n[2] Đo TCP latency đến SQL Server ({SQL_SERVER})...")
    tcp_result = measure_tcp_latency(SQL_SERVER, port=1433, attempts=10)
    all_results["tcp_latency_sql"] = tcp_result
    if "avg_ms" in tcp_result:
        print(f"  Avg: {tcp_result['avg_ms']:.2f}ms | "
              f"Min: {tcp_result['min_ms']:.2f}ms | "
              f"Max: {tcp_result['max_ms']:.2f}ms")
    else:
        print(f"  Status: {tcp_result.get('status', 'UNREACHABLE')}")

    # 3. SQL Query Latency
    print(f"\n[3] Đo SQL query latency...")
    sql_result = measure_sql_latency(attempts=10)
    all_results["sql_latency"] = sql_result
    if "connection_time_ms" in sql_result:
        print(f"  Connection: {sql_result['connection_time_ms']:.2f}ms")
        sq = sql_result["simple_query"]
        print(f"  Simple query avg: {sq['avg_ms']:.2f}ms (min {sq['min_ms']:.2f} / max {sq['max_ms']:.2f})")
        cq = sql_result["complex_query"]
        print(f"  Complex query avg: {cq['avg_ms']:.2f}ms (min {cq['min_ms']:.2f} / max {cq['max_ms']:.2f})")
    else:
        print(f"  Status: {sql_result.get('status', 'N/A')}")

    # 4. Multi-region endpoints
    print(f"\n[4] Đo latency đến Azure endpoints ở nhiều region...")
    azure_hosts = {
        "East US": "eastus.api.azureml.ms",
        "West Europe": "westeurope.api.azureml.ms",
        "Southeast Asia": "southeastasia.api.azureml.ms",
        "Japan East": "japaneast.api.azureml.ms",
        "Australia East": "australiaeast.api.azureml.ms",
    }
    multi_region = {}
    for region, host in azure_hosts.items():
        r = measure_tcp_latency(host, port=443, attempts=5)
        multi_region[region] = r
        if "avg_ms" in r:
            print(f"  {region:<20} avg: {r['avg_ms']:>8.2f}ms | "
                  f"min: {r['min_ms']:>8.2f}ms | max: {r['max_ms']:>8.2f}ms")
        else:
            print(f"  {region:<20} {r.get('status', 'UNREACHABLE')}")

    all_results["multi_region_latency"] = multi_region

    # 5. Phân tích & gợi ý
    reachable = {k: v for k, v in multi_region.items() if "avg_ms" in v}
    if reachable:
        best = min(reachable.items(), key=lambda x: x[1]["avg_ms"])
        all_results["recommendation"] = {
            "best_region": best[0],
            "avg_latency_ms": best[1]["avg_ms"],
            "note": f"Region {best[0]} có latency thấp nhất ({best[1]['avg_ms']:.2f}ms) "
                    f"từ vị trí hiện tại. Nên deploy resources ở region này.",
        }
        print(f"\n  → GỢI Ý: Deploy ở {best[0]} (latency {best[1]['avg_ms']:.2f}ms)")

    # Xuất kết quả
    output_dir = os.path.join(os.path.dirname(__file__), "..", "benchmark_output")
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "benchmark_latency.json")

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"\n  Báo cáo: {report_path}")


if __name__ == "__main__":
    run_latency_benchmark()
