##############################################################################
# Variables — MLOps Infrastructure
##############################################################################

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
}

variable "sql_admin_username" {
  description = "SQL Server admin username"
  type        = string
  sensitive   = true
}

variable "sql_admin_password" {
  description = "SQL Server admin password"
  type        = string
  sensitive   = true
  validation {
    condition     = length(var.sql_admin_password) >= 12
    error_message = "SQL password must be at least 12 characters."
  }
}

variable "ml_training_vm_size" {
  description = "VM size for ML training compute cluster"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "ml_endpoint_instance_type" {
  description = "Instance type for ML online endpoint"
  type        = string
  default     = "Standard_DS2_v2"
}
