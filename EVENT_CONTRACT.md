# Event Contract Specification

**Version**: 1.0  
**Last Updated**: May 26, 2026  
**Scope**: Complete real-time sales analytics pipeline

---

## 1. Overview

This document defines the single event contract used across the entire pipeline:

```
Data Generators → Event Hub → ValidateSalesEvent Function → Validated Event Hub 
    → Stream Analytics → SQL/Power BI
```

**Key Principle**: One contract, validated at entry point, assumed valid downstream.

---

## 2. Event Schema

### 2.1 Required Fields

All events **MUST** contain these fields:

| Field | Type | Format | Range/Values | Example |
|-------|------|--------|--------------|---------|
| `timestamp` | ISO 8601 string | `YYYY-MM-DDTHH:MM:SSZ` | ±24 hours from now | `2026-05-26T14:30:00Z` |
| `store_id` | string | Uppercase | `S01`, `S02`, `S03` | `S01` |
| `product_id` | string | Uppercase | See section 2.3 | `COKE` |
| `quantity` | integer | Positive int | 1–100 | `5` |
| `price` | number | Float, 2 decimals | 0.01–10,000.00 | `1.50` |

### 2.2 Optional Enrichment Fields

These fields are optional but if provided, must conform to the spec:

| Field | Type | Format | Range/Values | Default | Example |
|-------|------|--------|--------------|---------|---------|
| `temperature` | number | Float | -50 to 50 (°C) | 0 | `25.5` |
| `weather` | string | Lowercase | `sunny`, `rainy`, `cloudy`, `snowy` | `unknown` | `rainy` |
| `holiday` | integer | Binary | 0 or 1 | 0 | `1` |

### 2.3 Valid Product IDs (Single Source of Truth)

These IDs are defined in `config/settings.py::PRODUCTS` and synchronized across:
- ValidateSalesEvent function
- Stream Analytics Enriched CTE
- Web API (webapp/app.py)
- Reference data generators

**Product Categories**:

| Category | Product IDs |
|----------|-------------|
| **Beverage** | COKE, PEPSI, P016, P017 |
| **Dairy** | MILK, P019, P020 |
| **Bakery** | BREAD, P018 |
| **Electronics** | P001, P002, P003, P004, P005, P014, P015 |
| **Clothing** | P006, P007, P008 |
| **Home** | P009, P010, P011 |
| **Accessories** | P012, P013 |
| **Snacks** | P021, P022, P023 |
| **Health & Beauty** | P024, P025, P026 |
| **Sports** | P027, P028 |
| **Stationery** | P029, P030 |
| **Toys** | P031 |

### 2.4 Valid Store IDs

- `S01` – Ho Chi Minh City
- `S02` – Ha Noi
- `S03` – Da Nang

---

## 3. Event Lifecycle

### 3.1 Ingestion (Event Hub: sales-events)

**Source**: Data generators, web API  
**Format**: JSON, UTF-8 encoded  
**Example**:
```json
{
  "timestamp": "2026-05-26T14:30:00Z",
  "store_id": "S01",
  "product_id": "COKE",
  "quantity": 5,
  "price": 1.50,
  "temperature": 25.5,
  "weather": "rainy",
  "holiday": 0
}
```

### 3.2 Validation (Azure Function: ValidateSalesEvent)

**Checks performed**:
1. ✅ Required fields present
2. ✅ Field types correct (quantity=int, price=float)
3. ✅ Values in valid ranges
4. ✅ Timestamp within ±24 hours
5. ✅ Store/product IDs in whitelist
6. ✅ Duplicate detection (same event within same function instance)

**Invalid events**:
- Logged to `dbo.ValidationLogs` SQL table
- NOT forwarded to downstream pipeline
- Counted in Azure Monitor metrics

**Example error log**:
```json
{
  "error": "price out of range: 50000.00",
  "raw_data": "{...original event...}",
  "timestamp": "2026-05-26T14:30:15Z"
}
```

### 3.3 Cleaned Event (Event Hub: sales-events-validated)

After validation, events are enriched with computed fields:

```json
{
  "timestamp": "2026-05-26T14:30:00Z",
  "store_id": "S01",
  "product_id": "COKE",
  "quantity": 5,
  "price": 1.50,
  "revenue": 7.50,
  "temperature": 25.5,
  "weather": "rainy",
  "holiday": 0,
  "validated_at": "2026-05-26T14:30:15Z"
}
```

**New computed field**:
- `revenue` = `quantity × price`

**New tracking field**:
- `validated_at` = ISO 8601 timestamp when validation occurred

### 3.4 Processing (Stream Analytics)

Stream Analytics reads from `sales-events-validated` and performs:
1. Further type casting and null handling
2. Categorization lookup
3. 5-minute aggregations
4. Anomaly detection
5. Alert generation

**No re-validation** — assumes all events from validated hub are valid.

### 3.5 Output (SQL Server + Power BI)

**Tables**:
- `dbo.SalesTransactions` — individual transactions
- `dbo.HourlySalesSummary` — hourly rollup
- `dbo.SalesAlerts` — anomalies and alerts
- `dbo.ValidationLogs` — rejected events (for monitoring)

---

## 4. Backwards Compatibility

### Breaking Changes (v2.0+)

These would require synchronized updates across ALL components:
- Adding new **required** fields
- Removing existing fields
- Changing field names or types
- Changing store_id/product_id whitelist

### Non-Breaking Changes (within v1.0)

Safe to add without coordination:
- New **optional** fields (will be ignored by downstream if unused)
- New product IDs (add to `config/settings.py::PRODUCTS`)
- New stores (add to `config/settings.py::STORE_IDS`)

---

## 5. Testing the Contract

### 5.1 Unit Tests

```python
# tests/test_event_contract.py

from config.settings import VALID_PRODUCT_IDS, STORE_IDS

def test_event_schema():
    """Verify event contract matches config."""
    assert "COKE" in VALID_PRODUCT_IDS
    assert "S01" in STORE_IDS
```

### 5.2 Integration Tests

```bash
# Send test event to Event Hub
python scripts/test_event_contract.py --event '{"timestamp":"2026-05-26T14:30:00Z",...}'

# Verify in SQL
SELECT * FROM dbo.ValidationLogs WHERE logged_at > GETUTCDATE()-1
```

### 5.3 Contract Validation in CI/CD

Each deployment must:
1. ✅ Verify `config/settings.py` and Stream Analytics have matching product IDs
2. ✅ Verify Azure Function bindings are consistent
3. ✅ Verify Event Hub names match configurations

---

## 6. Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Events in ValidateSalesEvent but not in Stream Analytics | Connection string for `sales-events-validated` hub not set | Update `VALIDATED_EVENT_HUB_CONNECTION_STRING` in local.settings.json |
| "Unknown product_id" in ValidationLogs | New product added to generator but not in config.settings.py | Add product to PRODUCTS in config/settings.py |
| High validation error rate | Timestamp off by hours | Check system time on data generator host |
| No events in dbo.ValidationLogs | Function not configured to output invalid events | Verify function.json has `invalidEvents` output binding |

---

## 7. Ownership & Change Process

| Role | Responsibility |
|------|-----------------|
| **Data Owners** | Define valid product/store IDs (PRODUCTS, STORE_IDS in config) |
| **ML Team** | Define required ML input fields (in ml/score.py) |
| **Data Engineering** | Maintain consistency across pipeline components |
| **QA** | Test contract compliance in integration tests |

### Making Changes

1. **Proposal**: Document required change in GitHub issue
2. **Review**: All stakeholders (ML, Data Eng, QA) review
3. **Implementation**: Update in order:
   - `config/settings.py`
   - ValidateSalesEvent function
   - Stream Analytics query
   - Tests
   - Data generators
4. **Deployment**: Atomic release (all components together)

---

## 8. Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-05-26 | Initial contract with 5 required fields, 3 optional enrichment fields |

---

## Appendix A: Event Hub Configuration (Azure)

### Partitioning Strategy
- **Primary**: By `store_id` (ensures events from same store go to same partition)
- **Secondary**: Retention = 24 hours

### Consumer Groups
- `$Default` — ValidateSalesEvent function
- `stream-analytics` — Stream Analytics job

---

## Appendix B: Monitoring & Alerting

### Key Metrics

```kusto
// Azure Monitor query: Validation rate by store
CustomMetrics
| where name == "validation_rate"
| summarize Avg_Rate=avg(value) by store_id
| render columnchart
```

### Alert Rules

| Condition | Threshold | Action |
|-----------|-----------|--------|
| Validation error rate > 5% | per 5-min window | Email ops team |
| Missing events in validated hub | >100 consecutive | Page on-call |
| High latency (validation_time_ms) | >5000 | Trigger scaling |

---

## Questions?

Contact: @data-platform-team
Documentation: See PROJECT_DATAFLOW.md
