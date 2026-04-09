#!/usr/bin/env python3
"""
Add current IP to Azure SQL Server firewall rules.

Usage:
    python scripts/fix_sql_firewall.py
    python scripts/fix_sql_firewall.py --ip 14.164.42.245

Requires: az login (or DefaultAzureCredential)
"""

import argparse
import json
import urllib.request

from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient

SUBSCRIPTION_ID = "34849ef9-3814-44df-ba32-a86ed9f2a69a"
RESOURCE_GROUP = "rg-sales-analytics-dev"
SQL_SERVER = "sql-sales-analytics-d9bt2m"


def get_public_ip() -> str:
    """Detect current public IP address."""
    try:
        resp = urllib.request.urlopen("https://api.ipify.org?format=json", timeout=5)
        return json.loads(resp.read())["ip"]
    except Exception:
        return ""


def add_firewall_rule(ip: str, rule_name: str = "AllowCurrentIP"):
    """Add firewall rule to Azure SQL Server via REST API."""
    cred = DefaultAzureCredential()
    token = cred.get_token("https://management.azure.com/.default")

    url = (
        f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}"
        f"/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.Sql/servers/{SQL_SERVER}"
        f"/firewallRules/{rule_name}?api-version=2021-11-01"
    )

    body = json.dumps({
        "properties": {
            "startIpAddress": ip,
            "endIpAddress": ip,
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        },
    )

    resp = urllib.request.urlopen(req, timeout=15)
    result = json.loads(resp.read())
    print(f"✓ Firewall rule '{rule_name}' added: {ip}")
    return result


def add_allow_azure_services():
    """Allow Azure services to access SQL Server (special rule 0.0.0.0)."""
    cred = DefaultAzureCredential()
    token = cred.get_token("https://management.azure.com/.default")

    url = (
        f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}"
        f"/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.Sql/servers/{SQL_SERVER}"
        f"/firewallRules/AllowAllWindowsAzureIps?api-version=2021-11-01"
    )

    body = json.dumps({
        "properties": {
            "startIpAddress": "0.0.0.0",
            "endIpAddress": "0.0.0.0",
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        },
    )

    resp = urllib.request.urlopen(req, timeout=15)
    print("✓ Azure services access enabled (0.0.0.0 rule)")
    return json.loads(resp.read())


def main():
    parser = argparse.ArgumentParser(description="Fix SQL Server firewall")
    parser.add_argument("--ip", help="IP address to allow (auto-detect if omitted)")
    parser.add_argument("--rule-name", default="AllowCurrentIP", help="Firewall rule name")
    parser.add_argument("--allow-azure", action="store_true", help="Also allow Azure services")
    args = parser.parse_args()

    ip = args.ip or get_public_ip()
    if not ip:
        print("ERROR: Could not detect IP. Pass --ip manually.")
        return

    print(f"Adding firewall rule for IP: {ip}")
    add_firewall_rule(ip, args.rule_name)

    if args.allow_azure:
        add_allow_azure_services()

    print(f"\nDone! Wait up to 5 minutes for the rule to take effect.")
    print(f"Then retry Power BI refresh or SQL Query Editor.")


if __name__ == "__main__":
    main()
