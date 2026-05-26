# ─────────────────────────────────────────────
# Stream Analytics Inputs & Outputs
# Adds missing infrastructure to properly wire the pipeline
# ─────────────────────────────────────────────

# Create validated event hub (for output from ValidateSalesEvent function)
resource "azurerm_eventhub" "sales_validated" {
  name                = "${local.base_name}-validated"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 1

  tags = local.tags
}

# Consumer group for Stream Analytics to read validated events
resource "azurerm_eventhub_consumer_group" "sales_validated" {
  name                = "stream-analytics"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.sales_validated.name
  resource_group_name = azurerm_resource_group.main.name
}

# ─────────────────────────────────────────────
# Stream Analytics INPUT
# ─────────────────────────────────────────────

resource "azurerm_stream_analytics_stream_input_eventhub" "sales_input" {
  name                         = "SalesInput"
  stream_analytics_job_name    = azurerm_stream_analytics_job.main.name
  resource_group_name          = azurerm_resource_group.main.name
  eventhub_consumer_group_name = azurerm_eventhub_consumer_group.sales_validated.name
  eventhub_name                = azurerm_eventhub.sales_validated.name
  servicebus_namespace         = azurerm_eventhub_namespace.main.name
  shared_access_policy_key     = azurerm_eventhub_namespace.main.default_primary_key
  shared_access_policy_name    = "RootManageSharedAccessKey"

  serialization {
    type     = "Json"
    encoding = "UTF8"
  }
}

# ─────────────────────────────────────────────
# Stream Analytics OUTPUTS to SQL
# ─────────────────────────────────────────────

# Output 1: Individual Transactions
resource "azurerm_stream_analytics_output_mssql" "transactions" {
  name                       = "SalesTransactionsOutput"
  stream_analytics_job_name  = azurerm_stream_analytics_job.main.name
  resource_group_name        = azurerm_resource_group.main.name
  server                     = azurerm_mssql_server.main.fully_qualified_domain_name
  database                   = azurerm_mssql_database.main.name
  user                       = var.sql_admin_username
  password                   = var.sql_admin_password
  table                      = "dbo.SalesTransactions"
  max_batch_count            = 1000
  max_writer_count           = 10

  depends_on = [
    azurerm_mssql_database.main,
  ]
}

# Output 2: Hourly Summary
resource "azurerm_stream_analytics_output_mssql" "hourly_summary" {
  name                       = "HourlySalesSummaryOutput"
  stream_analytics_job_name  = azurerm_stream_analytics_job.main.name
  resource_group_name        = azurerm_resource_group.main.name
  server                     = azurerm_mssql_server.main.fully_qualified_domain_name
  database                   = azurerm_mssql_database.main.name
  user                       = var.sql_admin_username
  password                   = var.sql_admin_password
  table                      = "dbo.HourlySalesSummary"
  max_batch_count            = 100
  max_writer_count           = 5

  depends_on = [
    azurerm_mssql_database.main,
  ]
}

# Output 3: Alerts
resource "azurerm_stream_analytics_output_mssql" "alerts" {
  name                       = "SalesAlertsOutput"
  stream_analytics_job_name  = azurerm_stream_analytics_job.main.name
  resource_group_name        = azurerm_resource_group.main.name
  server                     = azurerm_mssql_server.main.fully_qualified_domain_name
  database                   = azurerm_mssql_database.main.name
  user                       = var.sql_admin_username
  password                   = var.sql_admin_password
  table                      = "dbo.SalesAlerts"
  max_batch_count            = 50
  max_writer_count           = 3

  depends_on = [
    azurerm_mssql_database.main,
  ]
}

# ─────────────────────────────────────────────
# Update Event Hub namespace connection strings for output
# ─────────────────────────────────────────────

# Authorized access for ValidateSalesEvent function output
resource "azurerm_eventhub_authorization_rule" "validate_function_sender" {
  name                = "ValidateSalesEventSender"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.sales_validated.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = false
  send                = true
  manage              = false
}

# Update: The validated event hub connection string should be stored
# in Key Vault for ValidateSalesEvent function to use
resource "azurerm_key_vault_secret" "validated_event_hub_connection" {
  name         = "ValidatedEventHubConnectionString"
  value        = azurerm_eventhub_authorization_rule.validate_function_sender.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id

  depends_on = [
    azurerm_key_vault_access_policy.terraform,
  ]
}
