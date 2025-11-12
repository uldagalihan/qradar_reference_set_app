#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from urllib.parse import quote
import requests
import truststore
from walkoff_app_sdk.app_base import AppBase

truststore.inject_into_ssl()

API_VERSION      = "21.0"
VERIFY_SSL       = False         # prod'da mümkünse True
HTTP_TIMEOUT_SEC = (5, 10)       # (connect, read)
MAX_HTTP_RETRIES = 5
BULK_BATCH_SIZE  = 1000
MAX_PER_SET      = 10_000

def ensure_trailing_api(base: str) -> str:
    base = (base or "").rstrip("/")
    if not base.endswith("/api"):
        base += "/api"
    return base

def make_headers(sec_token: str) -> dict:
    return {
        "SEC": sec_token,
        "Version": API_VERSION,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def http_request(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    timeout = kwargs.pop("timeout", HTTP_TIMEOUT_SEC)
    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            resp = session.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException:
            if attempt == MAX_HTTP_RETRIES:
                raise
            time.sleep(min(2 ** (attempt - 1), 10))
            continue

        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == MAX_HTTP_RETRIES:
                return resp
            ra = resp.headers.get("Retry-After")
            try:
                sleep_s = int(ra) if ra else min(2 ** (attempt - 1), 10)
            except ValueError:
                sleep_s = min(2 ** (attempt - 1), 10)
            time.sleep(sleep_s)
            continue

        return resp
    return resp

def find_set(session: requests.Session, base_url: str, set_name: str, headers: dict):
    filt = "name='%s'" % set_name
    url = f"{base_url}/reference_data_collections/sets?filter={quote(filt)}"
    r = http_request(session, "GET", url, headers=headers, verify=VERIFY_SSL)
    if r.status_code != 200:
        raise SystemExit(f"Set arama hatası ({set_name}) HTTP {r.status_code}: {r.text}")
    arr = r.json()
    if isinstance(arr, list) and arr:
        return arr[0]
    return None

def create_set(session, base_url, set_name, headers, entry_type="ALNIC"):
    # NO_EXPIRY default: TTL/expiry yok
    existing = find_set(session, base_url, set_name, headers)
    if existing and "id" in existing:
        return existing["id"]

    url = f"{base_url}/reference_data_collections/sets"
    body = {
        "name": set_name,
        "entry_type": entry_type,
        "expiry_type": "NO_EXPIRY",
    }
    r = http_request(session, "POST", url, headers=headers, json=body, verify=VERIFY_SSL)
    if r.status_code == 201:
        return r.json()["id"]
    if r.status_code == 409:
        existing = find_set(session, base_url, set_name, headers)
        if existing and "id" in existing:
            return existing["id"]
        raise SystemExit(f"409 ama ID bulunamadı: {set_name} -> {r.text}")
    raise SystemExit(f"Set oluşturulamadı: {set_name}  HTTP {r.status_code}: {r.text}")

def bulk_add_values(session, base_url, collection_id: int, values, headers,
                    source_label="shuffle_qradar_loader", wait_for_completion=False):
    patch_url  = f"{base_url}/reference_data_collections/set_entries"
    status_url = f"{base_url}/reference_data_collections/set_bulk_update_tasks/{{task_id}}"

    seen = set()
    values = [v for v in values if isinstance(v, str) and (v := v.strip()) and not (v in seen or seen.add(v))]

    task_ids = []
    for i in range(0, len(values), BULK_BATCH_SIZE):
        batch = values[i:i+BULK_BATCH_SIZE]
        payload = [{"collection_id": collection_id, "value": v, "source": source_label} for v in batch]
        r = http_request(session, "PATCH", patch_url, headers=headers, json=payload, verify=VERIFY_SSL)
        if r.status_code != 202:
            raise SystemExit(f"Bulk PATCH hata: HTTP {r.status_code} -> {r.text}")
        task_id = (r.json() or {}).get("id")
        if not task_id:
            raise SystemExit(f"Bulk task id dönmedi: {r.text}")
        task_ids.append(task_id)

    if not wait_for_completion:
        return {"task_ids": task_ids, "completed": False}

    start = time.time()
    for task_id in task_ids:
        while True:
            sr = http_request(session, "GET", status_url.format(task_id=task_id), headers=headers, verify=VERIFY_SSL)
            if sr.status_code != 200:
                raise SystemExit(f"Bulk task sorgu hata: HTTP {sr.status_code} -> {sr.text}")
            sjson = sr.json() or {}
            status = sjson.get("status", "")
            if status == "COMPLETED":
                break
            if status in ("EXCEPTION", "CONFLICT", "CANCELLED", "INTERRUPTED"):
                raise SystemExit(f"Bulk task hata: {status}  Detay: {json.dumps(sjson)[:400]}")
            if time.time() - start > 600:
                raise SystemExit(f"Bulk task timeout. Son: {json.dumps(sjson)[:400]}")
            time.sleep(1.0)

    return {"task_ids": task_ids, "completed": True}

def _values_from_items(items):
    """
    items beklenen format:
      - ["Ad Soyad", ...]  (string listesi)
      - [{"name":"Ad Soyad"}, ...]
    """
    out, seen = [], set()
    for it in (items or []):
        if isinstance(it, str):
            s = it.strip()
        elif isinstance(it, dict):
            s = (it.get("name") or "").strip()
        else:
            s = ""
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out

class QRadarReferenceSetApp(AppBase):
    __version__ = "2.1.0"
    app_name   = "QRadar Reference Set Loader (Name-only)"

    def __init__(self, redis=None, logger=None, **kwargs):
        super().__init__(redis=redis, logger=logger, **kwargs)

    # 🔹 SADE ACTION: sadece items alır (base alanlar + entry_type)
    def upsert_items(self,
                     base_url,
                     sec_token,
                     set_name="istenCikanlar",
                     entry_type="ALNIC",
                     items=None,
                     wait_for_completion=False):
        """
        Set'i (NO_EXPIRY) oluşturur/varsa kullanır ve 'items' içindeki adları ekler.
        items: ["Ad Soyad", ...] ya da [{"name":"Ad Soyad"}, ...]
        """
        base_url = ensure_trailing_api(base_url)
        session  = requests.Session()
        headers  = make_headers(sec_token)

        values = _values_from_items(items)
        if self.logger:
            self.logger.info(f"[QRadarLoader] items_in={len(items or [])} values_ready={len(values)}")

        if not values:
            return {"success": True, "inserted": 0, "set_names": [], "task_ids": [], "message": "No values."}

        set_names, task_ids_all, inserts = [], [], 0

        for i in range(0, len(values), MAX_PER_SET):
            part = values[i:i+MAX_PER_SET]
            this_set = set_name if i == 0 else f"{set_name}{(i//MAX_PER_SET)+1}"
            set_id = create_set(session, base_url, this_set, headers, entry_type=entry_type)
            set_names.append(this_set)

            res = bulk_add_values(session, base_url, set_id, part, headers=headers,
                                  wait_for_completion=wait_for_completion)
            task_ids_all.extend(res.get("task_ids", []))
            inserts += len(part)

        return {"success": True, "inserted": inserts, "set_names": set_names,
                "task_ids": task_ids_all, "message": "OK"}

if __name__ == "__main__":
    QRadarReferenceSetApp.run()
