#!/usr/bin/env python3
"""
fetch_c17_codes.py

Fetches NERC C17 JSON-LD data and writes a map of short codes:
{ "01AB": "SDN:C17::01AB" }
"""

import json
import requests

URL = "https://vocab.nerc.ac.uk/collection/C17/current/?_profile=nvs&_mediatype=application/ld+json"


def build_c17_code_map(data):
    """Build { short_code: full_code } dictionary."""
    c17_code_map = {}

    for item in data:
        if not isinstance(item, dict):
            continue

        c17_id = item.get("dc:identifier")
        if not c17_id or "::" not in c17_id:
            continue

        short_code = c17_id.split("::")[-1]
        c17_code_map[short_code] = c17_id

    return c17_code_map


def main():
    print("Fetching data from NERC vocabulary service...")
    response = requests.get(URL)
    response.raise_for_status()
    json_data = response.json()

    data = json_data.get("@graph", [])
    print(f"Loaded {len(data)} records from NERC C17 collection.")

    c17_code_map = build_c17_code_map(data)

    with open("c17_codes.json", "w", encoding="utf-8") as f:
        json.dump(c17_code_map, f, ensure_ascii=False, indent=2)

    print("\n✅ c17_codes.json written successfully.")


if __name__ == "__main__":
    main()
