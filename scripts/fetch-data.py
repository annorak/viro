#!/usr/bin/env python3
"""Fetch a bounded, attributed CDC extract for the Viro demo."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

root = Path(__file__).resolve().parents[1]
output = root / "data"
output.mkdir(exist_ok=True)
rows = []
queries = []
pathogens = ["SARS-CoV-2", "Influenza A virus", "RSV"]
for pathogen in pathogens:
    query = {
        "$limit": "400",
        "$order": "week_end DESC,site ASC",
        "$where": "pathogen_target='" + pathogen + "' AND source NOT LIKE '%WastewaterSCAN%'",
    }
    url = "https://data.cdc.gov/resource/atcp-73re.json?" + urlencode(query)
    queries.append(url)
    request = Request(url, headers={"User-Agent": "Viro local engineering demo"})
    with urlopen(request, timeout=45) as response:
        records = json.load(response)
    if not records:
        raise RuntimeError("No observations for " + pathogen)
    rows.extend(records)
keys = {(row["site"], row["pathogen_target"], row["week_end"]) for row in rows}
if len(keys) != len(rows):
    raise RuntimeError("Duplicate site/pathogen/week keys")
content = json.dumps(rows, separators=(",", ":")).encode()
manifest = {
    "dataset": "atcp-73re",
    "retrieved_at": datetime.now(timezone.utc).isoformat(),
    "source_policy": "Exclude rows whose source contains WastewaterSCAN",
    "rows": len(rows),
    "sha256": hashlib.sha256(content).hexdigest(),
    "queries": queries,
    "pathogens": {p: sum(row["pathogen_target"] == p for row in rows) for p in pathogens},
}
(output / "observations.json").write_bytes(content)
(output / "source-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(json.dumps(manifest, indent=2))
