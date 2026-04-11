##############################################################################
# Terraform — Retail Sales Forecasting MLOps Infrastructure
# Provision toàn bộ Azure resources cho hệ thống real-time analytics + MLOps
##############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Local backend cho môi trường dev/student
  # Để dùng remote backend, tạo Storage Account rồi uncomment block dưới:
  # backend "azurerm" {
  #   resource_group_name  = "rg-terraform-state"
  #   storage_account_name = "sttfsalesanalytics"
  #   container_name       = "tfstate"
  #   key                  = "sales-analytics.tfstate"
  # }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
  }
}

# ─────────────────────────────────────────────
# Data & Locals
# ─────────────────────────────────────────────
data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  suffix   = random_string.suffix.result
  base_name = "sales-analytics"
  tags = {
    project     = "retail-sales-forecasting"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ─────────────────────────────────────────────
# Resource Group
# ─────────────────────────────────────────────
resource "azurerm_resource_group" "main" {
  name     = "rg-${local.base_name}-${var.environment}"
  location = var.location
  tags     = local.tags
}

# ─────────────────────────────────────────────
# Log Analytics & Application Insights
# ─────────────────────────────────────────────
resource "azurerm_log_analytics_workspace" "main" {
  name                = "log-${local.base_name}-${local.suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.tags
}

resource "azurerm_application_insights" "main" {
  name                = "appi-${local.base_name}-${local.suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "web"
  tags                = local.tags
}

# ─────────────────────────────────────────────
# Storage Account
# ─────────────────────────────────────────────
resource "azurerm_storage_account" "main" {
  name                     = "st${replace(local.base_name, "-", "")}${local.suffix}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_container" "containers" {
  for_each              = toset(["reference-data", "sales-archive", "data-factory-staging", "ml-artifacts"])
  name                  = each.value
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

# ─────────────────────────────────────────────
# Key Vault
# ─────────────────────────────────────────────
resource "azurerm_key_vault" "main" {
  name                       = "kv-sales-${local.suffix}"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = false
  soft_delete_retention_days = 7

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = ["Get", "List", "Set", "Delete", "Purge"]
    key_permissions    = ["Get", "List", "Create"]
  }

  tags = local.tags
}

# ─────────────────────────────────────────────
# Event Hubs
# ─────────────────────────────────────────────
resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-${local.base_name}-${local.suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 1
  tags                = local.tags
}

resource "azurerm_eventhub" "sales" {
  name                = "sales-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 7
}

resource "azurerm_eventhub" "weather" {
  name                = "weather-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 2
  message_retention   = 7
}

resource "azurerm_eventhub" "stock" {
  name                = "stock-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 2
  message_retention   = 7
}

resource "azurerm_eventhub_consumer_group" "stream_analytics" {
  name                = "stream-analytics-cg"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.sales.name
  resource_group_name = azurerm_resource_group.main.name
}

# ─────────────────────────────────────────────
# Azure SQL Database
# ─────────────────────────────────────────────
resource "azurerm_mssql_server" "main" {
  name                         = "sql-${local.base_name}-${local.suffix}"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"
  tags                         = local.tags
}

resource "azurerm_mssql_database" "main" {
  name      = "SalesAnalyticsDB"
  server_id = azurerm_mssql_server.main.id
  sku_name  = "S0"
  tags      = local.tags
}

resource "azurerm_mssql_firewall_rule" "allow_azure" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# ─────────────────────────────────────────────
# Stream Analytics
# ─────────────────────────────────────────────
resource "azurerm_stream_analytics_job" "main" {
  name                                     = "sa-${local.base_name}-${local.suffix}"
  resource_group_name                      = azurerm_resource_group.main.name
  location                                 = azurerm_resource_group.main.location
  streaming_units                          = 3
  compatibility_level                      = "1.2"
  data_locale                              = "en-US"
  events_late_arrival_max_delay_in_seconds = 60
  events_out_of_order_max_delay_in_seconds = 50
  events_out_of_order_policy               = "Adjust"
  output_error_policy                      = "Drop"
  transformation_query                     = file("${path.module}/../stream_analytics/stream_query.sql")
  tags                                     = local.tags
}

# ─────────────────────────────────────────────
# Azure ML Workspace (MLOps Core)
# ─────────────────────────────────────────────
resource "azurerm_machine_learning_workspace" "main" {
  name                          = "aml-${local.base_name}-${local.suffix}"
  location                      = azurerm_resource_group.main.location
  resource_group_name           = azurerm_resource_group.main.name
  application_insights_id       = azurerm_application_insights.main.id
  key_vault_id                  = azurerm_key_vault.main.id
  storage_account_id            = azurerm_storage_account.main.id
  public_network_access_enabled = true

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# ML Compute Cluster for Training Pipeline
resource "azurerm_machine_learning_compute_cluster" "training" {
  name                          = "training-cluster"
  machine_learning_workspace_id = azurerm_machine_learning_workspace.main.id
  location                      = azurerm_resource_group.main.location
  vm_priority                   = "Dedicated"
  vm_size                       = var.ml_training_vm_size

  scale_settings {
    min_node_count                       = 0
    max_node_count                       = 4
    scale_down_nodes_after_idle_duration = "PT15M"
  }

  identity {
    type = "SystemAssigned"
  }
}

# ─────────────────────────────────────────────
# Azure Functions (Event Validation)
# ─────────────────────────────────────────────
resource "azurerm_service_plan" "functions" {
  name                = "asp-func-${local.suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  os_type             = "Linux"
  sku_name            = "Y1"
  tags                = local.tags
}

resource "azurerm_linux_function_app" "validation" {
  name                       = "func-sales-validation-${local.suffix}"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  service_plan_id            = azurerm_service_plan.functions.id
  storage_account_name       = azurerm_storage_account.main.name
  storage_account_access_key = azurerm_storage_account.main.primary_access_key

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.10"
    }
    application_insights_connection_string = azurerm_application_insights.main.connection_string
  }

  app_settings = {
    "KEY_VAULT_URL"              = azurerm_key_vault.main.vault_uri
    "EVENT_HUB_CONNECTION_STRING" = azurerm_eventhub_namespace.main.default_primary_connection_string
  }

  tags = local.tags
}

# ─────────────────────────────────────────────
# Key Vault Secrets (auto-populate)
# ─────────────────────────────────────────────
resource "azurerm_key_vault_secret" "event_hub_connection" {
  name         = "event-hub-connection-string"
  value        = azurerm_eventhub_namespace.main.default_primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "sql_connection" {
  name         = "sql-connection-string"
  value        = "Server=tcp:${azurerm_mssql_server.main.fully_qualified_domain_name},1433;Database=${azurerm_mssql_database.main.name};User ID=${var.sql_admin_username};Password=${var.sql_admin_password};Encrypt=yes;TrustServerCertificate=no;"
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "blob_connection" {
  name         = "blob-connection-string"
  value        = azurerm_storage_account.main.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "appinsights_connection" {
  name         = "appinsights-connection-string"
  value        = azurerm_application_insights.main.connection_string
  key_vault_id = azurerm_key_vault.main.id
}
