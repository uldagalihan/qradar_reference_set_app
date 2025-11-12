#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import json, time, re
from typing import Dict, List, Optional, Sequence
from urllib.parse import quote
import requests, truststore
from walkoff_app_sdk.app_base import AppBase

truststore.inject_into_ssl()

API_VERSION = "21.0"
VERIFY_SSL = False
HTTP_TIMEOUT_SEC = 60
POLL_INTERVAL_SEC = 1.0
POLL_TIMEOUT_SEC = 900
MAX_HTTP_RETRIES = 5
BULK_BATCH_SIZE = 1000
MAX_PER_SET = 10_000
DEFAULT_ENTRY_TYPES = ["ALNIC", "ALN"]

_TR_MAP = str.maketrans("çğıöşüÇĞİIÖŞÜ", "cgiosucgiiosu")
_SURNAME_CONNECTORS = {"de", "da", "van", "von", "bin", "ibn", "al", "el", "oğlu", "oglu", "del", "di"}

def normalize_username(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s).translate(_TR_MAP).lower()
    try:
        import unicodedata
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    s = s.replace(" ", ".").strip(".")
    return re.sub(r"\.+", ".", s)

def ensure_trailing_api(base: str) -> str:
    base = (base or "").rstrip("/")
    return base if base.endswith("/api") else base + "/api"

def make_headers(sec_token: str) -> Dict[str, str]:
    return {"SEC": sec_token, "Version": API_VERSION, "Accept": "application/json", "Content-Type": "application/json"}

def http_request(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    timeout = kwargs.pop("timeout", HTTP_TIMEOUT_SEC)
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException:
            if attempt == MAX_HTTP_RETRIES: raise
            time.sleep(min(2 ** (attempt - 1), 10)); continue
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == MAX_HTTP_RETRIES: return resp
            time.sleep(min(2 ** (attempt - 1), 10)); continue
        return resp
    raise RuntimeError("HTTP retry bitti ama yanıt yok.")

def find_set(session, base_url, set_name, headers):
    filt = f"name='{set_name}'"
    url = f"{base_url}/reference_data_collections/sets?filter={quote(filt)}"
    r = http_request(session, "GET", url, headers=headers, verify=VERIFY_SSL)
    if r.status_code != 200:
        raise SystemExit(f"Set arama hatası ({set_name}) HTTP {r.status_code}: {r.text}")
    arr = r.json()
    return arr[0] if isinstance(arr, list) and arr else None

def create_set(session, base_url, set_name, headers, entry_types=None):
    url = f"{base_url}/reference_data_collections/sets"
    entry_types = list(entry_types or DEFAULT_ENTRY_TYPES)
    last_err = None
    for et in entry_types:
        body = {"name": set_name, "entry_type": et}
        r = http_request(session, "POST", url, headers=headers, json=body, verify=VERIFY_SSL)
        if r.status_code == 201:
            return r.json()["id"]
        if r.status_code == 409:
            existing = find_set(session, base_url, set_name, headers)
            if existing and "id" in existing:
                return existing["id"]
        if r.status_code == 400:
            last_err = f"400 {r.text}"; continue
        raise SystemExit(f"Set oluşturulamadı: {set_name} HTTP {r.status_code}: {r.text}")
    raise SystemExit(f"Set oluşturma başarısız: {set_name} -> {last_err}")

def _dedup_keep_order(values):
    seen, out = set(), []
    for v in values:
        if v and v not in seen:
            seen.add(v); out.append(v)
    return out

def bulk_add_values(session, base_url, collection_id, values, headers, source_label="shuffle_qradar_loader"):
    patch_url = f"{base_url}/reference_data_collections/set_entries"
    status_url = f"{base_url}/reference_data_collections/set_bulk_update_tasks/{{task_id}}"
    values = _dedup_keep_order(values)
    for i in range(0, len(values), BULK_BATCH_SIZE):
        batch = values[i:i + BULK_BATCH_SIZE]
        payload = [{"collection_id": collection_id, "value": v, "source": source_label} for v in batch]
        r = http_request(session, "PATCH", patch_url, headers=headers, json=payload, verify=VERIFY_SSL)
        if r.status_code != 202:
            raise SystemExit(f"Bulk PATCH hata: HTTP {r.status_code} -> {r.text}")
        task_id = (r.json() or {}).get("id")
        if not task_id:
            raise SystemExit(f"Bulk task id dönmedi: {r.text}")
        start = time.time()
        while True:
            sr = http_request(session, "GET", status_url.format(task_id=task_id), headers=headers, verify=VERIFY_SSL)
            if sr.status_code != 200:
                raise SystemExit(f"Bulk task sorgu hata: HTTP {sr.status_code}: {sr.text}")
            sjson = sr.json() or {}
            status = sjson.get("status", "")
            if status == "COMPLETED": break
            if status in ("EXCEPTION", "CONFLICT", "CANCELLED", "INTERRUPTED"):
                raise SystemExit(f"Bulk task hata: {status} Detay: {json.dumps(sjson)[:400]}")
            if time.time() - start > POLL_TIMEOUT_SEC:
                raise SystemExit(f"Bulk task timeout ({POLL_TIMEOUT_SEC}s)")
            time.sleep(POLL_INTERVAL_SEC)

def split_name_tokens(full_name: str):
    return re.findall(r"[A-Za-zÇĞİÖŞÜçğıöşü'’-]+", full_name or "")

def detect_surname_tokens(tokens):
    if not tokens: return [], []
    if len(tokens) >= 2 and tokens[-2].lower() in _SURNAME_CONNECTORS:
        return tokens[:-2], tokens[-2:]
    return tokens[:-1], [tokens[-1]]

def name_variants(full_name: str):
    toks = split_name_tokens(full_name)
    if len(toks) < 2: return []
    given, surname = detect_surname_tokens(toks)
    if not given or not surname: return []
    surname_str = " ".join(surname)
    return _dedup_keep_order([normalize_username(f"{g} {surname_str}") for g in given])

def _parse_any_payload(maybe_json):
    data = maybe_json
    if isinstance(maybe_json, str):
        try: data = json.loads(maybe_json)
        except Exception: data = {}
    if not isinstance(data, dict): data = {}
    names, items = list(data.get("names") or []), list(data.get("items") or [])
    item_names = []
    for it in items:
        if isinstance(it, dict):
            nm = it.get("name") or it.get("full_name") or it.get("fullname") or ""
            if nm.strip(): item_names.append(nm.strip())
        elif isinstance(it, str) and it.strip(): item_names.append(it.strip())
    if not names and isinstance(maybe_json, (list, tuple)):
        names = [x for x in maybe_json if isinstance(x, str)]
    return {"names": _dedup_keep_order(names + item_names), "items": items}

def expand_from_items_or_names(payload):
    variants, items, names = [], payload.get("items") or [], payload.get("names") or []
    for it in items:
        nm = it.get("name") if isinstance(it, dict) else it
        if isinstance(nm, str): variants.extend(name_variants(nm))
    for nm in names:
        if isinstance(nm, str): variants.extend(name_variants(nm))
    return _dedup_keep_order(variants)

class QRadarReferenceSetApp(AppBase):
    __version__ = "1.0.1"
    app_name = "QRadar Reference Set Loader"

    def __init__(self, redis=None, logger=None, **kwargs):
        super().__init__(redis=redis, logger=logger, **kwargs)

    def upsert_from_payload(
        self, base_url, sec_token,
        base_set_name="istenCikanlar",
        entry_types=None,
        items=None, names=None,
        raw_payload=None,
        insert_full_names_if_empty=True
    ):
        parsed = _parse_any_payload(raw_payload) if raw_payload else {"names": [], "items": []}
        merged_names = list(names or []) + parsed.get("names", [])
        merged_items = list(items or []) + parsed.get("items", [])
        payload = {
            "base_url": ensure_trailing_api(base_url),
            "sec_token": sec_token,
            "base_set_name": base_set_name,
            "entry_types": list(entry_types or DEFAULT_ENTRY_TYPES),
            "items": merged_items,
            "names": merged_names,
        }
        try:
            variants = expand_from_items_or_names(payload)
            if not variants and insert_full_names_if_empty:
                variants = [n for n in merged_names if n.strip()]
            if not variants:
                return {"success": True, "inserted": 0, "set_names": [], "message": "No values to insert."}
            session = requests.Session(); headers = make_headers(payload["sec_token"])
            inserts, set_names = 0, []
            for i in range(0, len(variants), MAX_PER_SET):
                part = variants[i:i + MAX_PER_SET]
                set_name = base_set_name if i == 0 else f"{base_set_name}{(i // MAX_PER_SET) + 1}"
                set_names.append(set_name)
                set_id = create_set(session, payload["base_url"], set_name, headers, entry_types=payload["entry_types"])
                bulk_add_values(session, payload["base_url"], set_id, part, headers=headers)
                inserts += len(part)
            return {"success": True, "inserted": inserts, "set_names": set_names, "message": "OK"}
        except Exception as e:
            if self.logger: self.logger.exception("QRadar upsert_from_payload failed")
            return {"success": False, "inserted": 0, "set_names": [], "message": str(e)}

if __name__ == "__main__":
    QRadarReferenceSetApp.run()
