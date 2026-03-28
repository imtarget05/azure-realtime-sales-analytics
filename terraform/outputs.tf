##############################################################################
# Outputs — Resource info for CI/CD pipelines & downstream config
##############################################################################

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "ml_workspace_name" {
  value = azurerm_machine_learning_workspace.main.name
}

output "ml_workspace_id" {
  value = azurerm_machine_learning_workspace.main.id
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

output "storage_account_name" {
  value = azurerm_storage_account.main.name
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.main.fully_qualified_domain_name
}

output "sql_database_name" {
  value = azurerm_mssql_database.main.name
}

output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.main.name
}

output "stream_analytics_job_name" {
  value = azurerm_stream_analytics_job.main.name
}

output "application_insights_connection_string" {
  value     = azurerm_application_insights.main.connection_string
  sensitive = true
}

output "function_app_name" {
  value = azurerm_linux_function_app.validation.name
}

output "ml_training_cluster_name" {
  value = azurerm_machine_learning_compute_cluster.training.name
}
