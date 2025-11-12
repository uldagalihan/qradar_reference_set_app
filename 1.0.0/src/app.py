#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import re
from urllib.parse import quote
import requests
import truststore
from walkoff_app_sdk.app_base import AppBase

truststore.inject_into_ssl()   # use OS trust store

API_VERSION       = "21.0"
VERIFY_SSL        = False
HTTP_TIMEOUT_SEC  = 60
POLL_INTERVAL_SEC = 1.0
POLL_TIMEOUT_SEC  = 900
MAX_HTTP_RETRIES  = 5
BULK_BATCH_SIZE   = 1000
MAX_PER_SET       = 10_000
DEFAULT_TTL       = 15552000   # ~6 months
DEFAULT_ENTRY_TYPES = ["ALNIC", "ALN"]

_SURNAME_CONNECTORS = {"de","da","van","von","bin","ibn","al","el","oğlu","oglu","del","di"}

_TR_MAP = str.maketrans({
    "ç": "c", "ğ": "g", "ı": "i", "ö": "o", "ş": "s", "ü": "u",
    "Ç": "c", "Ğ": "g", "İ": "i", "I": "i", "Ö": "o", "Ş": "s", "Ü": "u",
})

def normalize_username(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\\s+", " ", s)
    s = s.translate(_TR_MAP).lower()
    try:
        import unicodedata
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    s = re.sub(r"\\s+", " ", s).strip()
    s = s.replace(" ", ".")
    s = re.sub(r"\\.+", ".", s).strip(".")
    return s

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

def create_set(session, base_url, set_name, headers, expiry_type="FIRST_SEEN", ttl=DEFAULT_TTL, entry_types=None):
    url = f"{base_url}/reference_data_collections/sets"
    last_err = None
    if not entry_types:
        entry_types = DEFAULT_ENTRY_TYPES

    for et in entry_types:
        body = {
            "name": set_name,
            "entry_type": et,
            "expiry_type": expiry_type,
            "time_to_live": ttl,
        }
        r = http_request(session, "POST", url, headers=headers, json=body, verify=VERIFY_SSL)
        if r.status_code == 201:
            return r.json()["id"]
        if r.status_code == 409:
            existing = find_set(session, base_url, set_name, headers)
            if existing and "id" in existing:
                return existing["id"]
            raise SystemExit(f"409 ama ID bulunamadı: {set_name} -> {r.text}")
        if r.status_code == 400:
            last_err = f"400 {r.text}"
            continue
        raise SystemExit(f"Set oluşturulamadı: {set_name}  HTTP {r.status_code}: {r.text}")

    raise SystemExit(f"Set oluşturma başarısız: {set_name}  Son hata: {last_err}")

def bulk_add_values(session, base_url, collection_id: int, values, headers, source_label="shuffle_qradar_loader"):
    patch_url  = f"{base_url}/reference_data_collections/set_entries"
    status_url = f"{base_url}/reference_data_collections/set_bulk_update_tasks/{{task_id}}"

    seen = set()
    values = [v for v in values if v and not (v in seen or seen.add(v))]

    for i in range(0, len(values), BULK_BATCH_SIZE):
        batch = values[i:i+BULK_BATCH_SIZE]
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
                raise SystemExit(f"Bulk task sorgu hata: HTTP {sr.status_code} -> {sr.text}")
            sjson = sr.json() or {}
            status = sjson.get("status", "")
            if status == "COMPLETED":
                break
            if status in ("EXCEPTION", "CONFLICT", "CANCELLED", "INTERRUPTED"):
                raise SystemExit(f"Bulk task hata: {status}  Detay: {json.dumps(sjson)[:400]}")
            if time.time() - start > POLL_TIMEOUT_SEC:
                raise SystemExit(f"Bulk task timeout ({POLL_TIMEOUT_SEC}s). Son: {json.dumps(sjson)[:400]}")
            time.sleep(POLL_INTERVAL_SEC)

def split_name_tokens(full_name: str):
    toks = re.findall(r"[A-Za-zÇĞİÖŞÜçğıöşü'’-]+", full_name or "")
    return toks

def detect_surname_tokens(tokens):
    if not tokens:
        return [], []
    if len(tokens) >= 2 and tokens[-2].lower() in _SURNAME_CONNECTORS:
        return tokens[:-2], tokens[-2:]
    return tokens[:-1], [tokens[-1]]

def name_variants(full_name: str):
    toks = split_name_tokens(full_name)
    if len(toks) < 2:
        return []
    given, surname = detect_surname_tokens(toks)
    if not given or not surname:
        return []
    surname_str = " ".join(surname)
    out = []
    for g in given:
        u = normalize_username(f"{g} {surname_str}")
        if u:
            out.append(u)
    seen = set()
    return [v for v in out if not (v in seen or seen.add(v))]

def expand_from_items_or_names(payload: dict):
    variants = []
    items = payload.get("items") or []
    names = payload.get("names") or []
    if isinstance(items, list):
        for it in items:
            nm = (it or {}).get("name") or ""
            variants.extend(name_variants(nm))
    if isinstance(names, list):
        for nm in names:
            variants.extend(name_variants(nm))
    seen = set()
    return [v for v in variants if v and not (v in seen or seen.add(v))]

class QRadarReferenceSetApp(AppBase):
    __version__ = "1.0.0"
    app_name   = "QRadar Reference Set Loader"

    def __init__(self, redis=None, logger=None, **kwargs):
        super().__init__(redis=redis, logger=logger, **kwargs)

    def upsert_from_payload(self, base_url, sec_token, base_set_name="istenCikanlar",
                            expiry_type="FIRST_SEEN", time_to_live=DEFAULT_TTL, entry_types=None,
                            items=None, names=None):
        """
        Shuffle action:
        - base_url: "https://<qradar>/api" ("/api" yoksa eklenir)
        - sec_token: QRadar SEC token
        - base_set_name: default "istenCikanlar"
        - expiry_type: FIRST_SEEN | LAST_SEEN | NO_EXPIRY
        - time_to_live: seconds (default 6 months)
        - entry_types: ["ALNIC","ALN"] order
        - items: Outlook JSON list (optional)
        - names: list of full names (optional)
        """
        payload = {
            "base_url": base_url,
            "sec_token": sec_token,
            "base_set_name": base_set_name or "istenCikanlar",
            "expiry_type": expiry_type or "FIRST_SEEN",
            "time_to_live": int(time_to_live) if time_to_live is not None else DEFAULT_TTL,
            "entry_types": entry_types or DEFAULT_ENTRY_TYPES,
            "items": items or [],
            "names": names or [],
        }

        try:
            variants = expand_from_items_or_names(payload)
            if self.logger:
                self.logger.info(f"[QRadarLoader] variants={len(variants)}")

            if not variants:
                return {"success": True, "inserted": 0, "set_names": [], "message": "No values to insert."}

            base_url = ensure_trailing_api(payload["base_url"])
            session  = requests.Session()
            headers  = make_headers(payload["sec_token"])

            # 10k shard: base, base2, base3, ...
            inserts = 0
            set_names = []

            for i in range(0, len(variants), MAX_PER_SET):
                part = variants[i:i+MAX_PER_SET]
                set_name = payload["base_set_name"] if i == 0 else f"{payload['base_set_name']}{(i//MAX_PER_SET)+1}"
                set_names.append(set_name)

                set_id = create_set(session, base_url, set_name, headers,
                                    expiry_type=payload["expiry_type"],
                                    ttl=payload["time_to_live"],
                                    entry_types=payload["entry_types"])

                bulk_add_values(session, base_url, set_id, part, headers=headers)
                inserts += len(part)

            return {"success": True, "inserted": inserts, "set_names": set_names, "message": "OK"}

        except Exception as e:
            if self.logger:
                self.logger.exception("QRadar upsert_from_payload failed")
            return {"success": False, "inserted": 0, "set_names": [], "message": str(e)}

if __name__ == "__main__":
    QRadarReferenceSetApp.run()
