"""
Azure Data Factory - Tạo và quản lý pipeline.
Data Factory đóng vai trò điều phối (orchestration):
  1. Copy dữ liệu từ Blob Storage staging vào Azure SQL
  2. Trigger huấn luyện ML model theo lịch
  3. Chạy stored procedures trên SQL Database
"""

import os
import sys
import json

sys.path.insert(0, ".")
from config.settings import (
    AZURE_SUBSCRIPTION_ID,
    AZURE_RESOURCE_GROUP,
    AZURE_LOCATION,
    SQL_SERVER,
    SQL_DATABASE,
    SQL_USERNAME,
    SQL_PASSWORD,
)

try:
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.mgmt.datafactory.models import (
        Factory,
        LinkedServiceResource,
        AzureSqlDatabaseLinkedService,
        AzureBlobStorageLinkedService,
        AzureMLServiceLinkedService,
        DatasetResource,
        AzureSqlTableDataset,
        AzureBlobDataset,
        LinkedServiceReference,
        PipelineResource,
        CopyActivity,
        BlobSource,
        SqlSink,
        SqlServerStoredProcedureActivity,
        AzureMLExecutePipelineActivity,
        ActivityDependency,
        DependencyCondition,
        TriggerResource,
        ScheduleTrigger,
        ScheduleTriggerRecurrence,
        TriggerPipelineReference,
        PipelineReference,
    )
except ImportError:
    print("[ERROR] Cần cài đặt: pip install azure-mgmt-datafactory azure-identity")
    sys.exit(1)

# ========================
# CẤU HÌNH
# ========================
DATA_FACTORY_NAME = os.getenv("DATA_FACTORY_NAME", "adf-sales-analytics")
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING", "<Your-Blob-Connection-String>")
AML_WORKSPACE_NAME = os.getenv("AML_WORKSPACE_NAME", "<your-aml-workspace>")


def get_adf_client() -> DataFactoryManagementClient:
    """Tạo Data Factory Management Client."""
    credential = DefaultAzureCredential()
    return DataFactoryManagementClient(credential, AZURE_SUBSCRIPTION_ID)


def create_data_factory(client: DataFactoryManagementClient):
    """Bước 1: Tạo Data Factory instance."""
    print("[1/6] Tạo Data Factory...")
    factory = Factory(location=AZURE_LOCATION)
    result = client.factories.create_or_update(
        AZURE_RESOURCE_GROUP, DATA_FACTORY_NAME, factory
    )
    print(f"  [OK] Data Factory '{result.name}' đã tạo tại {result.location}")
    return result


def create_linked_services(client: DataFactoryManagementClient):
    """Bước 2: Tạo Linked Services (kết nối đến các dịch vụ)."""
    print("[2/6] Tạo Linked Services...")

    # 2a. Azure Blob Storage Linked Service
    blob_ls = LinkedServiceResource(
        properties=AzureBlobStorageLinkedService(
            connection_string=BLOB_CONNECTION_STRING
        )
    )
    client.linked_services.create_or_update(
        AZURE_RESOURCE_GROUP, DATA_FACTORY_NAME,
        "AzureBlobStorageLS", blob_ls
    )
    print("  [OK] Linked Service: AzureBlobStorageLS (Blob Storage)")

    # 2b. Azure SQL Database Linked Service
    sql_conn_string = (
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"User ID={SQL_USERNAME};"
        f"Password={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    sql_ls = LinkedServiceResource(
        properties=AzureSqlDatabaseLinkedService(
            connection_string=sql_conn_string
        )
    )
    client.linked_services.create_or_update(
        AZURE_RESOURCE_GROUP, DATA_FACTORY_NAME,
        "AzureSqlDatabaseLS", sql_ls
    )
    print("  [OK] Linked Service: AzureSqlDatabaseLS (SQL Database)")

    # 2c. Azure ML Linked Service
    aml_ls = LinkedServiceResource(
        properties=AzureMLServiceLinkedService(
            subscription_id=AZURE_SUBSCRIPTION_ID,
            resource_group_name=AZURE_RESOURCE_GROUP,
            ml_workspace_name=AML_WORKSPACE_NAME,
        )
    )
    client.linked_services.create_or_update(
        AZURE_RESOURCE_GROUP, DATA_FACTORY_NAME,
        "AzureMLServiceLS", aml_ls
    )
    print("  [OK] Linked Service: AzureMLServiceLS (Machine Learning)")


def create_datasets(client: DataFactoryManagementClient):
    """Bước 3: Tạo Datasets (nguồn và đích dữ liệu)."""
    print("[3/6] Tạo Datasets...")

    # 3a. Blob Storage Dataset - staging CSV files
    blob_dataset = DatasetResource(
        properties=AzureBlobDataset(
            linked_service_name=LinkedServiceReference(
                type="LinkedServiceReference",
                reference_name="AzureBlobStorageLS"
            ),
            folder_path="data-factory-staging",
            format={
                "type": "JsonFormat",
            },
        )
    )
    client.datasets.create_or_update(
        AZURE_RESOURCE_GROUP, DATA_FACTORY_NAME,
        "BlobStagingDataset", blob_dataset
    )
    print("  [OK] Dataset: BlobStagingDataset")

    # 3b. Azure SQL Dataset - SalesTransactions table
    sql_dataset = DatasetResource(
        properties=AzureSqlTableDataset(
            linked_service_name=LinkedServiceReference(
                type="LinkedServiceReference",
                reference_name="AzureSqlDatabaseLS"
            ),
            table_name="dbo.SalesTransactions",
        )
    )
    client.datasets.create_or_update(
        AZURE_RESOURCE_GROUP, DATA_FACTORY_NAME,
        "SqlSalesDataset", sql_dataset
    )
    print("  [OK] Dataset: SqlSalesDataset")

    # 3c. Azure SQL Dataset - SalesForecast table
    forecast_dataset = DatasetResource(
        properties=AzureSqlTableDataset(
            linked_service_name=LinkedServiceReference(
                type="LinkedServiceReference",
                reference_name="AzureSqlDatabaseLS"
            ),
            table_name="dbo.SalesForecast",
        )
    )
    client.datasets.create_or_update(
        AZURE_RESOURCE_GROUP, DATA_FACTORY_NAME,
        "SqlForecastDataset", forecast_dataset
    )
    print("  [OK] Dataset: SqlForecastDataset")


def create_sales_analytics_pipeline(client: DataFactoryManagementClient):
    """Bước 4: Tạo Pipeline SalesAnalyticsPipeline với 4 activities."""
    print("[4/5] Tạo Pipeline: SalesAnalyticsPipeline...")

    # Activity 1: Copy dữ liệu từ Blob staging vào SQL
    copy_activity = CopyActivity(
        name="CopyBlobToSQL",
        source=BlobSource(),
        sink=SqlSink(write_behavior="insert"),
        inputs=[{
            "referenceName": "BlobStagingDataset",
            "type": "DatasetReference",
        }],
        outputs=[{
            "referenceName": "SqlSalesDataset",
            "type": "DatasetReference",
        }],
    )

    # Activity 2: Chuẩn bị training data (depends on CopyBlobToSQL)
    prepare_data = SqlServerStoredProcedureActivity(
        name="PrepareTrainingData",
        linked_service_name=LinkedServiceReference(
            type="LinkedServiceReference",
            reference_name="AzureSqlDatabaseLS"
        ),
        stored_procedure_name="sp_PrepareTrainingData",
        depends_on=[
            ActivityDependency(
                activity="CopyBlobToSQL",
                dependency_conditions=[DependencyCondition.SUCCEEDED],
            )
        ],
    )

    # Activity 3: Trigger ML Pipeline (depends on PrepareTrainingData)
    run_ml = AzureMLExecutePipelineActivity(
        name="RunMLPipeline",
        linked_service_name=LinkedServiceReference(
            type="LinkedServiceReference",
            reference_name="AzureMLServiceLS"
        ),
        ml_pipeline_id=os.getenv("AML_PIPELINE_ID", "sales-retrain-pipeline-v1"),
        depends_on=[
            ActivityDependency(
                activity="PrepareTrainingData",
                dependency_conditions=[DependencyCondition.SUCCEEDED],
            )
        ],
    )

    # Activity 4: Cập nhật forecasts (depends on RunMLPipeline)
    update_forecasts = SqlServerStoredProcedureActivity(
        name="UpdateForecasts",
        linked_service_name=LinkedServiceReference(
            type="LinkedServiceReference",
            reference_name="AzureSqlDatabaseLS"
        ),
        stored_procedure_name="sp_UpdateForecasts",
        depends_on=[
            ActivityDependency(
                activity="RunMLPipeline",
                dependency_conditions=[DependencyCondition.SUCCEEDED],
            )
        ],
    )

    pipeline = PipelineResource(
        activities=[copy_activity, prepare_data, run_ml, update_forecasts],
        description="Pipeline tổng hợp: Copy dữ liệu Blob → SQL → ML Prediction → Update Forecasts",
    )

    client.pipelines.create_or_update(
        AZURE_RESOURCE_GROUP, DATA_FACTORY_NAME,
        "SalesAnalyticsPipeline", pipeline
    )
    print("  [OK] Pipeline: SalesAnalyticsPipeline (4 activities)")


def create_scheduled_trigger(client: DataFactoryManagementClient):
    """Bước 5: Tạo Trigger chạy pipeline theo lịch."""
    print("[5/5] Tạo Scheduled Trigger...")

    # Trigger chạy SalesAnalyticsPipeline mỗi ngày lúc 2:00 AM UTC
    trigger = TriggerResource(
        properties=ScheduleTrigger(
            description="Trigger chạy SalesAnalyticsPipeline hàng ngày",
            pipelines=[
                TriggerPipelineReference(
                    pipeline_reference=PipelineReference(
                        type="PipelineReference",
                        reference_name="SalesAnalyticsPipeline"
                    )
                )
            ],
            recurrence=ScheduleTriggerRecurrence(
                frequency="Day",
                interval=1,
                start_time="2026-03-01T02:00:00Z",
                time_zone="UTC",
                schedule={
                    "hours": [2],
                    "minutes": [0],
                },
            ),
        )
    )

    client.triggers.create_or_update(
        AZURE_RESOURCE_GROUP, DATA_FACTORY_NAME,
        "DailyMLTrigger", trigger
    )
    print("  [OK] Trigger: DailyMLTrigger (hàng ngày lúc 02:00 UTC)")


def main():
    print("=" * 60)
    print("  TRIỂN KHAI AZURE DATA FACTORY")
    print(f"  Factory: {DATA_FACTORY_NAME}")
    print(f"  Resource Group: {AZURE_RESOURCE_GROUP}")
    print("=" * 60)

    client = get_adf_client()

    create_data_factory(client)
    create_linked_services(client)
    create_datasets(client)
    create_sales_analytics_pipeline(client)
    create_scheduled_trigger(client)

    print("\n" + "=" * 60)
    print("  DATA FACTORY TRIỂN KHAI HOÀN TẤT!")
    print("  Pipeline:")
    print("    - SalesAnalyticsPipeline (4 activities):")
    print("      1. CopyBlobToSQL: Blob → SQL Database")
    print("      2. PrepareTrainingData: sp_PrepareTrainingData")
    print("      3. RunMLPipeline: Azure ML Pipeline")
    print("      4. UpdateForecasts: sp_UpdateForecasts")
    print("  Trigger:")
    print("    - DailyMLTrigger: Chạy SalesAnalyticsPipeline mỗi ngày 02:00 UTC")
    print("=" * 60)


if __name__ == "__main__":
    main()
