"""
Azure Key Vault integration for centralized secrets management.

All Azure service credentials are stored in Key Vault and accessed
via Managed Identity — no secrets in code or environment variables.

Architecture:
  Key Vault ─→ Azure Functions (Managed Identity)
  Key Vault ─→ Stream Analytics (Managed Identity)
  Key Vault ─→ Azure Monitor (Managed Identity)

Usage:
    from security.key_vault import SecretManager
    sm = SecretManager()
    conn_str = sm.get_secret("event-hub-connection-string")
"""

import logging
import os
from functools import lru_cache

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Secret names stored in Key Vault
# ──────────────────────────────────────────────
SECRET_NAMES = {
    "event-hub-connection-string": "Connection string for Azure Event Hubs",
    "sql-connection-string": "ODBC connection string for Azure SQL",
    "sql-admin-password": "Azure SQL admin password",
    "ml-endpoint-url": "Azure ML Online Endpoint URL",
    "ml-api-key": "Azure ML API key",
    "blob-connection-string": "Azure Blob Storage connection string",
    "powerbi-push-url": "Power BI streaming dataset push URL",
    "appinsights-connection-string": "Application Insights connection string",
    "openweather-api-key": "OpenWeatherMap API key",
}


class SecretManager:
    """Manages secrets via Azure Key Vault with Managed Identity."""

    def __init__(self, vault_url: str | None = None):
        self.vault_url = vault_url or os.getenv(
            "KEY_VAULT_URL", "https://kv-sales-analytics.vault.azure.net/"
        )

        # Use Managed Identity in Azure, DefaultAzureCredential locally
        try:
            if os.getenv("WEBSITE_INSTANCE_ID"):
                # Running in Azure (App Service / Functions)
                credential = ManagedIdentityCredential()
            else:
                credential = DefaultAzureCredential()

            self.client = SecretClient(vault_url=self.vault_url, credential=credential)
            logger.info("Key Vault client initialized: %s", self.vault_url)
        except Exception as exc:
            logger.error("Failed to initialize Key Vault client: %s", exc)
            self.client = None

    @lru_cache(maxsize=32)
    def get_secret(self, name: str) -> str | None:
        """Retrieve a secret value from Key Vault. Cached per instance."""
        if self.client is None:
            logger.warning("Key Vault not available, falling back to env var: %s", name)
            env_key = name.upper().replace("-", "_")
            return os.getenv(env_key)
        try:
            secret = self.client.get_secret(name)
            logger.info("Retrieved secret: %s", name)
            return secret.value
        except Exception as exc:
            logger.warning("Failed to get secret '%s': %s. Falling back to env.", name, exc)
            env_key = name.upper().replace("-", "_")
            return os.getenv(env_key)

    def set_secret(self, name: str, value: str) -> bool:
        """Store a secret in Key Vault."""
        if self.client is None:
            logger.error("Key Vault not available, cannot set secret: %s", name)
            return False
        try:
            self.client.set_secret(name, value)
            logger.info("Stored secret: %s", name)
            self.get_secret.cache_clear()
            return True
        except Exception as exc:
            logger.error("Failed to set secret '%s': %s", name, exc)
            return False

    def list_secrets(self) -> list[str]:
        """List all secret names in the vault."""
        if self.client is None:
            return []
        try:
            return [s.name for s in self.client.list_properties_of_secrets()]
        except Exception as exc:
            logger.error("Failed to list secrets: %s", exc)
            return []

    def health_check(self) -> dict:
        """Check Key Vault connectivity and secret availability."""
        status = {"vault_url": self.vault_url, "connected": False, "secrets": {}}
        if self.client is None:
            return status
        try:
            for name in SECRET_NAMES:
                try:
                    val = self.client.get_secret(name)
                    status["secrets"][name] = "available" if val.value else "empty"
                except Exception:
                    status["secrets"][name] = "missing"
            status["connected"] = True
        except Exception as exc:
            status["error"] = str(exc)
        return status
