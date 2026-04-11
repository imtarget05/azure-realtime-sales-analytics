import json, sys
data = json.load(sys.stdin)
header = "{:<45} {:>5} {:>5}".format("Family", "Used", "Limit")
print(header)
print("-" * 57)
for item in data:
    limit = int(item["limit"])
    current = int(item["currentValue"])
    name = item["name"]["localizedValue"]
    if limit > 0 and "Standard" in name and "Family" in name:
        print("{:<45} {:>5} {:>5}".format(name, current, limit))
