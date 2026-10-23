"""AC-8.8: run every panel query of the provisioned dashboards through Grafana's /api/ds/query.

No query may return an error.

Usage: GRAFANA_ADMIN_PASSWORD=… python3 scripts/check_dashboards.py   (standard library only)
"""

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://127.0.0.1:3000"
AUTH = "Basic " + base64.b64encode(f"admin:{os.environ['GRAFANA_ADMIN_PASSWORD']}".encode()).decode()


def query(target: dict) -> tuple[bool, str, int]:
    body = {"from": "now-6h", "to": "now", "queries": [{**target, "maxDataPoints": 100, "intervalMs": 30000}]}
    req = urllib.request.Request(
        f"{BASE}/api/ds/query",
        json.dumps(body).encode(),
        method="POST",
        headers={"Authorization": AUTH, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            result = json.load(r)["results"][target["refId"]]
    except urllib.error.HTTPError as e:
        return False, e.read().decode()[:200], 0
    if result.get("error"):
        return False, result["error"][:200], 0
    columns = [((f.get("data") or {}).get("values") or []) for f in result.get("frames", [])]
    rows = sum(len(c[0]) if c and c[0] else 0 for c in columns)  # an empty frame is a valid "no rows" result
    return True, "", rows


failures = 0
for path in sorted(Path("infra/observability/grafana/dashboards").glob("*.json")):
    dash = json.loads(path.read_text())
    for panel in dash["panels"]:
        for t in panel["targets"]:
            # $__range is a dashboard macro; resolve it the way Grafana would for the 6 h range.
            target = {k: (v.replace("$__range", "6h") if isinstance(v, str) else v) for k, v in t.items()}
            ok, err, rows = query(target)
            print(
                f"{'PASS' if ok else 'FAIL'} {dash['uid']} / {panel['title']} "
                f"[{t['refId']}] rows={rows} {err}"
            )
            failures += not ok
print(f"check_dashboards: {failures} failure(s)")
sys.exit(1 if failures else 0)
