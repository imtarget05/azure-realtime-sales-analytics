# Azure Realtime Sales Analytics - Data Flow Architecture

## Project Overview
- **Description**: Event-driven real-time sales analytics system on Microsoft Azure
- **Language Composition**: Python (86.7%) | HTML (6%) | TSQL (3.1%) | HCL (1.5%) | PowerShell (1.4%) | Shell (1.2%) | Other (0.1%)
- **Focus**: Stream processing, cloud architecture, interactive visualization

## Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────┐
│        AZURE REALTIME SALES ANALYTICS (Event-Driven)         │
└──────────────────────────────────────────────────────────────┘

DATA SOURCES
    │
    ├─ Sales Transactions
    ├─ Point of Sale (POS) Systems
    ├─ E-commerce Platforms
    ├─ Inventory Management
    └─ Customer Interactions

EVENT INGESTION
    │
    ├─ Azure Event Hubs
    ├─ Message Brokers
    │   └─ Kafka/RabbitMQ
    │
    ▼

STREAM PROCESSING (Python - 86.7%)
    │
    ├─ Apache Spark / Azure Stream Analytics
    ├─ Event Parsing & Validation
    │   ├─ Schema Validation
    │   ├─ Data Type Conversion
    │   └─ Error Handling
    │
    ├─ Real-Time Transformations
    │   ├─ Aggregations
    │   │   ├─ Sum of Sales
    │   │   ├─ Transaction Count
    │   │   ├─ Average Order Value
    │   │   └─ Time-based Windows
    │   │
    │   ├─ Filtering & Enrichment
    │   │   ├─ Filter by Region
    │   │   ├─ Join with Master Data
    │   │   └─ Add Calculated Fields
    │   │
    │   └─ Anomaly Detection
    │       ├─ Spike Detection
    │       ├─ Trend Analysis
    │       └─ Outlier Identification
    │
    ▼

DATA PERSISTENCE
    │
    ├─ Azure SQL Database (TSQL - 3.1%)
    │   ├─ Aggregated Metrics
    │   ├─ Historical Data
    │   ├─ Fact Tables
    │   └─ Dimension Tables
    │
    ├─ Azure Data Lake Storage
    │   ├─ Raw Events
    │   ├─ Processed Data
    │   └─ Archive
    │
    ├─ Azure Cosmos DB
    │   └─ Time-Series Data
    │
    ├─ Redis Cache
    │   └─ Hot Data Cache
    │
    ▼

ANALYTICS & REPORTING (Python - 86.7% + TSQL - 3.1%)
    │
    ├─ Data Warehouse Queries
    ├─ KPI Calculations
    │   ├─ Revenue Metrics
    │   ├─ Sales Performance
    │   ├─ Customer Metrics
    │   └─ Product Performance
    │
    ├─ Machine Learning Models (Python)
    │   ├─ Sales Forecasting
    │   ├─ Customer Segmentation
    │   ├─ Demand Prediction
    │   └─ Churn Analysis
    │
    ▼

VISUALIZATION LAYER (HTML - 6%)
    │
    ├─ Interactive Dashboards
    ├─ Real-Time Charts
    │   ├─ Line Charts (Trends)
    │   ├─ Bar Charts (Comparisons)
    │   ├─ Pie Charts (Distribution)
    │   └─ Heat Maps (Patterns)
    │
    ├─ KPI Cards
    ├─ Drilldown Capabilities
    └─ Filtered Views

INFRASTRUCTURE AS CODE (HCL - 1.5%)
    │
    ├─ Terraform Scripts
    ├─ Resource Provisioning
    │   ├─ Virtual Machines
    │   ├─ Storage Accounts
    │   ├─ Databases
    │   ├─ Event Hubs
    │   └─ Container Registry
    │
    └─ Network Configuration

AUTOMATION & DEPLOYMENT (PowerShell - 1.4%, Shell - 1.2%)
    │
    ├─ Deployment Scripts
    ├─ Scheduling Jobs
    ├─ Backup Automation
    ├─ Monitoring & Alerting Setup
    └─ CI/CD Pipeline Triggers

MONITORING & ALERTS
    │
    ├─ Azure Monitor
    ├─ Application Insights
    ├─ Log Analytics
    ├─ Real-Time Anomalies
    └─ Performance Metrics

OUTPUT
    │
    └─ Stakeholder Dashboards
        ├─ Executive Summary
        ├─ Operational Reports
        ├─ Regional Analysis
        └─ Product Performance Reports
```

## Technology Stack
- **Core Processing**: Python (86.7%)
- **Database Queries**: TSQL (3.1%)
- **Infrastructure**: HCL (1.5%)
- **Automation**: PowerShell (1.4%) + Shell (1.2%)
- **Visualization**: HTML (6%)

## Key Data Transformations
1. Raw Events → Parsed Events
2. Events → Windowed Aggregations
3. Aggregations → KPIs
4. KPIs → Dimensional Data
5. Dimensional Data → Fact Tables
6. Fact Tables → Visualizations
7. Historical Data → Forecasts & Predictions
8. All Data → Executive Dashboards

## Azure Services Architecture
```
Event Sources → Event Hubs → Stream Analytics → 
SQL Database / Data Lake → Power BI / Custom Dashboard
                    ↓
              Machine Learning Models
                    ↓
            Predictions & Insights
```
