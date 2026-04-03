# Kien Truc Tong The (Ban Bao Ve)

Tai lieu nay dung cho slide bao ve, bo sung ro 2 luong quan trong:
- Drift Monitor -> Auto Retrain -> Promote model
- Key Vault -> cap secret cho app/pipeline thay vi hardcode

## Mermaid Diagram

```mermaid
flowchart LR
    A[Data Generator\nSales Weather Stock] --> B[Azure Event Hubs]
    B --> C[Azure Stream Analytics]
    C --> D1[(Azure SQL\nSalesTransactions)]
    C --> D2[(Azure SQL\nHourlySalesSummary)]
    C --> D3[(Azure SQL\nSalesAlerts)]

    D1 --> V1[(vw_RealtimeDashboard)]
    D1 --> V2[(vw_ForecastVsActual)]
    D2 --> V2

    V2 --> M1[ml/drift_monitor.py]
    M1 -->|MAE > threshold| M2[ml/retrain_and_compare.py --promote]
    M1 -->|optional| M3[mlops/trigger_training_pipeline.py]

    M2 --> MO[(ml/model_output\nmodel + report)]
    M3 --> AML[Azure ML Pipeline/Endpoint]
    AML --> MO

    MO --> W[Flask Web App\n/model-report + /predict]
    D3 --> W
    D1 --> PBI[Power BI Dashboard]

    KV[Azure Key Vault] --> S[config/settings.py]
    S --> M1
    S --> W
    S --> C

    ADF[Azure Data Factory] --> AML
    ADF --> D1
```

## Checklist nhanh truoc demo

1. Stream Analytics da map output `SalesAlertsOutput` -> bang `dbo.SalesAlerts`.
2. SQL da tao bang `SalesAlerts` va view `vw_ForecastVsActual`.
3. Key Vault co cac secret:
   - `sql-admin-password`
   - `event-hub-connection-string`
   - `blob-connection-string`
4. Chay `python ml/drift_monitor.py --trigger-mode both` de kiem tra vong CT.
5. Mo `http://localhost:5000/model-report` de trinh bay ket qua retrain moi nhat.
