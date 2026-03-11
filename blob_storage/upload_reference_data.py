"""
Upload dữ liệu tham chiếu (reference data) lên Azure Blob Storage.
Blob Storage phục vụ:
  1. Reference data cho Stream Analytics (danh sách sản phẩm, vùng...)
  2. Lưu trữ archive dữ liệu từ Event Hub Capture
  3. Staging area cho Data Factory pipeline
"""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, ".")
from config.settings import PRODUCTS, REGIONS, CUSTOMER_SEGMENTS, PAYMENT_METHODS

try:
    from azure.storage.blob import BlobServiceClient, ContentSettings
    from azure.identity import DefaultAzureCredential
except ImportError:
    print("[ERROR] Cần cài đặt: pip install azure-storage-blob azure-identity")
    sys.exit(1)


# ========================
# CẤU HÌNH
# ========================
BLOB_CONNECTION_STRING = os.getenv(
    "BLOB_CONNECTION_STRING",
    "<Your-Blob-Storage-Connection-String>"
)
CONTAINER_REFERENCE = "reference-data"
CONTAINER_ARCHIVE = "sales-archive"
CONTAINER_STAGING = "data-factory-staging"


def create_containers(blob_service: BlobServiceClient):
    """Tạo các container cần thiết trong Blob Storage."""
    containers = [CONTAINER_REFERENCE, CONTAINER_ARCHIVE, CONTAINER_STAGING]
    for name in containers:
        try:
            blob_service.create_container(name)
            print(f"  [OK] Container '{name}' đã tạo.")
        except Exception as e:
            if "ContainerAlreadyExists" in str(e):
                print(f"  [OK] Container '{name}' đã tồn tại.")
            else:
                print(f"  [WARN] Container '{name}': {e}")


def upload_product_reference(blob_service: BlobServiceClient):
    """Upload danh sách sản phẩm làm reference data cho Stream Analytics."""
    products_data = []
    for p in PRODUCTS:
        products_data.append({
            "product_id": p["id"],
            "product_name": p["name"],
            "category": p["category"],
            "base_price": p["base_price"],
        })

    data_json = json.dumps(products_data, indent=2)
    blob_client = blob_service.get_blob_client(
        container=CONTAINER_REFERENCE,
        blob="products/products.json"
    )
    blob_client.upload_blob(
        data_json,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    print(f"  [OK] Uploaded products.json ({len(products_data)} sản phẩm)")


def upload_regions_reference(blob_service: BlobServiceClient):
    """Upload danh sách vùng làm reference data."""
    regions_data = [
        {"region": r, "timezone": "UTC", "country": "Vietnam"}
        for r in REGIONS
    ]
    data_json = json.dumps(regions_data, indent=2)
    blob_client = blob_service.get_blob_client(
        container=CONTAINER_REFERENCE,
        blob="regions/regions.json"
    )
    blob_client.upload_blob(
        data_json,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    print(f"  [OK] Uploaded regions.json ({len(regions_data)} vùng)")


def upload_customer_segments_reference(blob_service: BlobServiceClient):
    """Upload danh sách phân khúc khách hàng."""
    segments_data = [
        {"segment": s, "discount_eligible": s in ["Premium", "VIP"]}
        for s in CUSTOMER_SEGMENTS
    ]
    data_json = json.dumps(segments_data, indent=2)
    blob_client = blob_service.get_blob_client(
        container=CONTAINER_REFERENCE,
        blob="segments/customer_segments.json"
    )
    blob_client.upload_blob(
        data_json,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    print(f"  [OK] Uploaded customer_segments.json ({len(segments_data)} phân khúc)")


def upload_payment_methods_reference(blob_service: BlobServiceClient):
    """Upload danh sách phương thức thanh toán."""
    payments_data = [
        {"method": m, "is_digital": m not in ["Cash"]}
        for m in PAYMENT_METHODS
    ]
    data_json = json.dumps(payments_data, indent=2)
    blob_client = blob_service.get_blob_client(
        container=CONTAINER_REFERENCE,
        blob="payments/payment_methods.json"
    )
    blob_client.upload_blob(
        data_json,
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    print(f"  [OK] Uploaded payment_methods.json ({len(payments_data)} phương thức)")


def main():
    print("=" * 60)
    print("  UPLOAD REFERENCE DATA LÊN AZURE BLOB STORAGE")
    print("=" * 60)

    if BLOB_CONNECTION_STRING.startswith("<"):
        print("[ERROR] Chưa cấu hình BLOB_CONNECTION_STRING trong .env")
        print("  Chạy script deploy trước hoặc cập nhật file .env")
        sys.exit(1)

    blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)

    print("\n[1/5] Tạo containers...")
    create_containers(blob_service)

    print("\n[2/5] Upload products reference data...")
    upload_product_reference(blob_service)

    print("\n[3/5] Upload regions reference data...")
    upload_regions_reference(blob_service)

    print("\n[4/5] Upload customer segments reference data...")
    upload_customer_segments_reference(blob_service)

    print("\n[5/5] Upload payment methods reference data...")
    upload_payment_methods_reference(blob_service)

    print("\n" + "=" * 60)
    print("  HOÀN TẤT!")
    print("  Reference data đã sẵn sàng cho Stream Analytics.")
    print("  Containers đã tạo:")
    print(f"    - {CONTAINER_REFERENCE}: Dữ liệu tham chiếu")
    print(f"    - {CONTAINER_ARCHIVE}: Archive từ Event Hub Capture")
    print(f"    - {CONTAINER_STAGING}: Staging cho Data Factory")
    print("=" * 60)


if __name__ == "__main__":
    main()
