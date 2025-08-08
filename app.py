import os
import json
import time
import requests
from flask import Flask, Response
from typing import Dict, Any, List

# ========= Env vars =========
NOTION_TOKEN        = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID  = os.environ["NOTION_DATABASE_ID"]

# Property names (exact, but override-able via env)
PROP_NETWORK_NAME   = os.environ.get("NOTION_PROP_NETWORK_NAME", "Network Name")
PROP_POLYGON        = os.environ.get("NOTION_PROP_POLYGON", "Polygon")
PROP_LEADERS        = os.environ.get("NOTION_PROP_LEADERS", "Network Leaders Names")

NOTION_API = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ========= Helpers: generic text extraction =========
def _plain_from_rich_or_title(prop: Dict[str, Any]) -> str:
    if "rich_text" in prop:
        return "".join([r.get("plain_text", "") for r in prop.get("rich_text", [])]).strip()
    if "title" in prop:
        return "".join([r.get("plain_text", "") for r in prop.get("title", [])]).strip()
    return ""

def _plain_from_select(prop: Dict[str, Any]) -> str:
    sel = prop.get("select")
    return sel["name"] if sel else ""

def _plain_from_multi_select(prop: Dict[str, Any]) -> str:
    arr = prop.get("multi_select", [])
    return ", ".join(opt.get("name", "") for opt in arr if opt.get("name"))

def _plain_from_people(prop: Dict[str, Any]) -> str:
    arr = prop.get("people", [])
    names = []
    for u in arr:
        # Notion "people" objects usually have 'name'; fallback to email.
        names.append(u.get("name") or (u.get("person") or {}).get("email", ""))
    return ", ".join([n for n in names if n])

def _plain_from_rollup(prop: Dict[str, Any]) -> str:
    r = prop.get("rollup", {})
    t = r.get("type")
    if t == "array":
        vals = []
        for item in r.get("array", []):
            vals.append(_read_text_flex(item))
        return ", ".join([v for v in vals if v])
    if t == "number":
        return str(r.get("number", ""))
    if t == "date":
        d = r.get("date") or {}
        return d.get("start") or ""
    # fallback
    return ""

def _read_text_flex(prop: Dict[str, Any]) -> str:
    """Return human-readable text from many Notion property types."""
    if "select" in prop:
        return _plain_from_select(prop)
    if "multi_select" in prop:
        return _plain_from_multi_select(prop)
    if "people" in prop:
        return _plain_from_people(prop)
    if "rollup" in prop:
        return _plain_from_rollup(prop)
    # fallbacks: rich_text or title
    return _plain_from_rich_or_title(prop)

# ========= Polygon parsing =========
def _read_polygon_geometry(prop: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expect polygon JSON stored as text in a rich_text or title property.
    Returns a GeoJSON geometry dict with 'type' and 'coordinates'.
    """
    raw = _plain_from_rich_or_title(prop)
    if not raw:
        raise ValueError("Polygon field is empty")
    try:
        geom = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Polygon JSON parse error: {e}")
    # Minimal validation
    if not isinstance(geom, dict) or "type" not in geom or "coordinates" not in geom:
        raise ValueError("Polygon must be a GeoJSON geometry object with type/coordinates")
    return geom

# ========= Notion fetch =========
def fetch_all_pages() -> List[Dict[str, Any]]:
    """Query the Notion DB with pagination; respect rate limits (3 req/sec)."""
    pages = []
    payload: Dict[str, Any] = {"page_size": 100}
    while True:
        resp = requests.post(NOTION_API, headers=HEADERS, json=payload, timeout=30)
        if resp.status_code == 429:
            time.sleep(1)
            continue
        resp.raise_for_status()
        data = resp.json()
        pages.extend(data.get("results", []))
        if data.get("has_more"):
            payload["start_cursor"] = data["next_cursor"]
            time.sleep(0.35)  # be polite to Notion
        else:
            break
    return pages

# ========= GeoJSON build =========
def build_geojson(pages: List[Dict[str, Any]]) -> Dict[str, Any]:
    features = []
    for p in pages:
        props = p.get("properties", {})
        try:
            if PROP_POLYGON not in props:
                raise KeyError(f"Missing '{PROP_POLYGON}' property")

            geom = _read_polygon_geometry(props[PROP_POLYGON])

            network_name = _read_text_flex(props.get(PROP_NETWORK_NAME, {})) if PROP_NETWORK_NAME in props else ""
            leaders      = _read_text_flex(props.get(PROP_LEADERS, {})) if PROP_LEADERS in props else ""

            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "Network": network_name,
                    "Leaders": leaders
                }
            })
        except Exception as err:
            print(f"⚠️ Skipping page {p.get('id','?')}: {err}")
            continue

    return {"type": "FeatureCollection", "features": features}

# ========= Flask app =========
app = Flask(__name__)

@app.route("/")
def serve_geojson():
    pages = fetch_all_pages()
    fc = build_geojson(pages)
    return Response(
        json.dumps(fc),
        mimetype="application/geo+json",
        headers={
            "Content-Disposition": 'inline; filename="Notion-Networks.geojson"'
        }
    )

@app.route("/health")
def health():
    return {"ok": True}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
