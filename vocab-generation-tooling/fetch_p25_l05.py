import json
import time
from typing import Dict, List
import requests

HEADERS = {"Accept": "application/ld+json"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

P25_COLLECTION_URL = (
    "https://vocab.nerc.ac.uk/collection/P25/current/"
    "?_profile=nvs&_mediatype=application/ld+json"
)

OUTPUT_FILE = "p25_to_l05_codes.json"


def get_json(url: str) -> dict:
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def extract_code(uri: str) -> str:
    """Extract concept code from NVS URI."""
    return uri.rstrip("/").split("/")[-1]


def extract_p25_ids(collection_json: dict) -> List[str]:
    ids = []
    for item in collection_json.get("@graph", []):
        uri = item.get("@id")
        if uri and "/P25/" in uri:
            ids.append(uri)
    return sorted(set(ids))


def extract_related_l05_codes(concept_json: dict) -> List[str]:
    codes = []

    related = concept_json.get("skos:related")
    if not related:
        return []

    if isinstance(related, dict):
        related = [related]

    for item in related:
        uri = item.get("@id")
        if uri and "/L05/" in uri:
            codes.append(extract_code(uri))

    return sorted(set(codes))


def main():
    print("Fetching P25 collection…")
    collection_json = get_json(P25_COLLECTION_URL)
    p25_ids = extract_p25_ids(collection_json)

    print(f"Found {len(p25_ids)} P25 concepts")

    mapping: Dict[str, List[str]] = {}

    for i, p25_uri in enumerate(p25_ids, start=1):
        p25_code = extract_code(p25_uri)
        print(f"[{i}/{len(p25_ids)}] {p25_code}")

        url = f"{p25_uri}?_profile=nvs&_mediatype=application/ld+json"
        concept_json = get_json(url)

        l05_codes = extract_related_l05_codes(concept_json)
        mapping[p25_code] = l05_codes

        time.sleep(0.15)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2)

    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()