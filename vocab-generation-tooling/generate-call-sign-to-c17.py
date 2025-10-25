#!/usr/bin/env python3
"""
fetch_callsign_map.py

Fetches NERC C17 JSON-LD data and writes a callsign map:
{ callsign: [ { "c17": ..., "commissioned": <ISO>, "decommissioned": <ISO> }, ... ] }

Only processes records where skos:definition is a JSON object with @value.
"""

import json
import requests
from datetime import datetime

URL = "https://vocab.nerc.ac.uk/collection/C17/current/?_profile=nvs&_mediatype=application/ld+json"


def to_iso_timestamp(value):
    """Convert a year or date string into ISO 8601 format (UTC)."""
    if value is None:
        return None

    try:
        # If it's an integer or numeric string representing a year
        year = int(value)
        return f"{year:04d}-01-01T00:00:00Z"
    except (ValueError, TypeError):
        pass

    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                dt = datetime.strptime(value.strip(), fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue

    return None


def build_callsign_map(data):
    """Build callsign → [ship entries] map."""
    callsign_map = {}

    for item in data:
        if not isinstance(item, dict):
            continue

        # Must have a valid C17 code
        c17_id = item.get("dc:identifier")
        if not c17_id:
            continue

        # Only accept structured skos:definition
        definition = item.get("skos:definition")
        if not isinstance(definition, dict):
            continue

        definition_str = definition.get("@value")
        if not definition_str:
            continue

        # Try parsing definition JSON content
        try:
            node = json.loads(definition_str).get("node", {})
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue

        callsign = node.get("callsign")
        if not callsign:
            continue

        entry = {
            "c17": c17_id,
            "commissioned": to_iso_timestamp(node.get("commissioned")),
            "decommissioned": to_iso_timestamp(node.get("decommissioned")),
        }

        callsign_map.setdefault(callsign, []).append(entry)

    return callsign_map


def main():
    print("Fetching data from NERC vocabulary service...")
    response = requests.get(URL)
    response.raise_for_status()
    json_data = response.json()

    data = json_data.get("@graph", [])
    print(f"Loaded {len(data)} records from NERC C17 collection.")

    callsign_map = build_callsign_map(data)

    with open("callsign_map.json", "w", encoding="utf-8") as f:
        json.dump(callsign_map, f, ensure_ascii=False, indent=2)

    print(f"\n✅ callsign_map.json written successfully. ({len(callsign_map)} callsigns found)")


if __name__ == "__main__":
    main()
