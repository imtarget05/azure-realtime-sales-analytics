import json, subprocess
result = subprocess.run(
    ['az', 'resource', 'list', '--resource-group', 'rg-sales-analytics-dev', '--output', 'json'],
    capture_output=True, text=True
)
data = json.loads(result.stdout)
print(f"{'TYPE':55} NAME")
print("-" * 100)
for r in data:
    print(f"{r['type']:55} {r['name']}")
