#!/usr/bin/env python3
"""
airtable_to_notion.py
=====================

Migrate one Airtable base's tables into Notion databases via API-to-API
transfer, preserving field types and rebuilding linked-record fields as live
Notion relations rather than flattening them to text.

Targets the Notion API version 2025-09-03, in which a "database" is a container
and the actual table of records is a "data source". Pages are created under a
`data_source_id` parent, and relation properties point at data source IDs.

Two-pass design (single run):
  Pass 1  Read each Airtable table's schema + records -> create a Notion
          database with the scalar properties -> insert a page per record,
          recording an airtable_record_id -> notion_page_id map.
  Pass 2  For each Airtable linked-record field, add a relation property to the
          source data source pointing at the target data source (resolved from
          the field's linkedTableId in the schema), then patch each page to set
          the actual links.

Airtable makes both passes simpler than a Coda source would: the Metadata API
returns the full typed schema (field types, select choices, and each linked
field's target table) up front, and linked-record values are plain arrays of
record IDs.

Resumable: progress is checkpointed to migration_state_<base_id>.json as it
goes. Re-run with the same AIRTABLE_BASE_ID to pick up where an interrupted run
left off. Use --restart to discard saved progress (does NOT delete databases
already created in Notion).

Attachments: image/file fields are stored as their URLs in a text property.
Airtable attachment URLs are time-limited, so treat them as a record of what was
attached rather than durable links.

Credentials and targets are read from environment variables (a .env file is
supported). Copy .env.example to .env and fill it in:

  AIRTABLE_API_TOKEN     Airtable personal access token (PAT) with scopes
                         schema.bases:read and data.records:read, granted access
                         to the base you are migrating
  AIRTABLE_BASE_ID       Base id (starts with "app"); see the API docs or the
                         base URL: airtable.com/appXXXXXXXX/...
  NOTION_API_TOKEN       Notion internal integration secret
  NOTION_PARENT_PAGE_ID  Notion page the databases are created under; the
                         integration must be connected to this page

Run:
  python airtable_to_notion.py
  python airtable_to_notion.py --skip "Imported table, Archive"
  python airtable_to_notion.py --only "Projects, Clients"
  python airtable_to_notion.py --restart

Requirements:  Python 3.9+, `pip install -r requirements.txt`
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import logging
from dataclasses import dataclass, field
from typing import Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("airtable2notion")

AIRTABLE_BASE = "https://api.airtable.com/v0"
AIRTABLE_META = "https://api.airtable.com/v0/meta"
NOTION_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2025-09-03"

SAVE_EVERY = 25          # checkpoint progress every N inserted rows
TEXT_LIMIT = 2000        # Notion rich_text content cap per text object
MAX_SELECT_OPTIONS = 100


# --------------------------------------------------------------------------- #
# HTTP plumbing: throttle + retry                                             #
# --------------------------------------------------------------------------- #

class Throttle:
    """Minimum-interval throttle to respect per-service rate limits."""

    def __init__(self, min_interval: float) -> None:
        self.min_interval = min_interval
        self._last = 0.0

    def wait(self) -> None:
        delta = time.monotonic() - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.monotonic()


def _request(
    session: requests.Session,
    method: str,
    url: str,
    throttle: Throttle,
    *,
    max_retries: int = 6,
    **kwargs: Any,
) -> requests.Response:
    """Issue a request with throttling and backoff on 429/5xx and network errors."""
    kwargs.setdefault("timeout", 90)
    for attempt in range(max_retries):
        throttle.wait()
        try:
            resp = session.request(method, url, **kwargs)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt == max_retries - 1:
                raise
            delay = min(2 ** attempt, 30)
            log.warning(
                "%s %s -> network error (%s); retrying in %.1fs (attempt %d/%d)",
                method, url, type(exc).__name__, delay, attempt + 1, max_retries,
            )
            time.sleep(delay)
            continue
        if resp.status_code < 400:
            return resp
        if resp.status_code == 429 or resp.status_code >= 500:
            retry_after = resp.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else min(2 ** attempt, 30)
            log.warning(
                "%s %s -> %s; retrying in %.1fs (attempt %d/%d)",
                method, url, resp.status_code, delay, attempt + 1, max_retries,
            )
            time.sleep(delay)
            continue
        # Notion sometimes returns a transient 400 right after creating a
        # database ("Unsaved transactions") while its backend settles.
        if resp.status_code == 400 and "Unsaved transactions" in resp.text:
            delay = min(2 ** attempt, 30)
            log.warning(
                "%s %s -> transient 400; retrying in %.1fs (attempt %d/%d)",
                method, url, delay, attempt + 1, max_retries,
            )
            time.sleep(delay)
            continue
        raise RuntimeError(f"{method} {url} failed {resp.status_code}: {resp.text}")
    raise RuntimeError(f"{method} {url} exhausted retries")


# --------------------------------------------------------------------------- #
# Airtable client                                                             #
# --------------------------------------------------------------------------- #

class AirtableClient:
    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        self.throttle = Throttle(0.22)  # ~5 requests/second per base

    def list_tables(self, base_id: str) -> list[dict]:
        """Full schema for every table in the base (fields, types, options)."""
        url = f"{AIRTABLE_META}/bases/{base_id}/tables"
        return _request(self.session, "GET", url, self.throttle).json().get("tables", [])

    def list_records(self, base_id: str, table_id: str) -> list[dict]:
        """All records for a table, keyed by field id, following pagination."""
        url = f"{AIRTABLE_BASE}/{base_id}/{table_id}"
        records: list[dict] = []
        params = {"pageSize": 100, "returnFieldsByFieldId": "true"}
        while True:
            data = _request(self.session, "GET", url, self.throttle, params=params).json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            params = {"pageSize": 100, "returnFieldsByFieldId": "true", "offset": offset}
        return records


# --------------------------------------------------------------------------- #
# Notion client                                                               #
# --------------------------------------------------------------------------- #

class NotionClient:
    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        })
        self.throttle = Throttle(0.34)  # ~3 requests/second

    def _post(self, path: str, body: dict) -> dict:
        return _request(self.session, "POST", f"{NOTION_BASE}{path}", self.throttle, json=body).json()

    def _patch(self, path: str, body: dict) -> dict:
        return _request(self.session, "PATCH", f"{NOTION_BASE}{path}", self.throttle, json=body).json()

    def _get(self, path: str) -> dict:
        return _request(self.session, "GET", f"{NOTION_BASE}{path}", self.throttle).json()

    def create_database(
        self, parent_page_id: str, title: str, properties: dict
    ) -> tuple[str, str]:
        """Create a database + initial data source. Returns (db_id, data_source_id)."""
        body = {
            "parent": {"type": "page_id", "page_id": parent_page_id},
            "title": [{"type": "text", "text": {"content": title[:TEXT_LIMIT]}}],
            "initial_data_source": {"properties": properties},
        }
        resp = self._post("/databases", body)
        db_id = resp["id"]
        sources = resp.get("data_sources") or []
        if sources:
            return db_id, sources[0]["id"]
        got = self._get(f"/databases/{db_id}")
        return db_id, got["data_sources"][0]["id"]

    def create_page(self, data_source_id: str, properties: dict) -> str:
        body = {
            "parent": {"type": "data_source_id", "data_source_id": data_source_id},
            "properties": properties,
        }
        return self._post("/pages", body)["id"]

    def add_relation_property(
        self, data_source_id: str, prop_name: str, target_data_source_id: str
    ) -> None:
        body = {
            "properties": {
                prop_name: {
                    "type": "relation",
                    "relation": {
                        "data_source_id": target_data_source_id,
                        "single_property": {},
                    },
                }
            }
        }
        self._patch(f"/data_sources/{data_source_id}", body)

    def set_page_relation(
        self, page_id: str, prop_name: str, target_page_ids: list[str]
    ) -> None:
        body = {"properties": {prop_name: {"relation": [{"id": p} for p in target_page_ids]}}}
        self._patch(f"/pages/{page_id}", body)


# --------------------------------------------------------------------------- #
# Value flattening                                                            #
# --------------------------------------------------------------------------- #

def flatten(value: Any) -> Any:
    """Reduce an Airtable field value to a scalar (or list of scalars)."""
    if isinstance(value, list):
        return [flatten(v) for v in value]
    if isinstance(value, dict):
        # Barcode -> text; collaborator -> name/email; attachment/button -> url.
        if "text" in value:
            return value["text"]
        if "name" in value:
            return value["name"]
        if "email" in value:
            return value["email"]
        if "url" in value:
            return value["url"]
        if "label" in value:
            return value["label"]
        return json.dumps(value, ensure_ascii=False)
    return value


def to_text(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(flatten(v)) for v in value if v not in (None, ""))
    return "" if value is None else str(value)


def sanitize_option(name: str) -> str:
    # Notion select/multi_select option names may not contain commas.
    return name.replace(",", " ").strip()[:TEXT_LIMIT]


# --------------------------------------------------------------------------- #
# Airtable field type -> Notion property mapping                              #
# --------------------------------------------------------------------------- #

SCALAR_MAP = {
    "singleLineText": "rich_text",
    "multilineText": "rich_text",
    "richText": "rich_text",
    "email": "email",
    "url": "url",
    "phoneNumber": "phone_number",
    "number": "number",
    "currency": "number",
    "percent": "number",
    "duration": "number",
    "rating": "number",
    "count": "number",
    "autoNumber": "number",
    "date": "date",
    "dateTime": "date",
    "createdTime": "date",
    "lastModifiedTime": "date",
    "checkbox": "checkbox",
    "singleSelect": "select",
    "multipleSelects": "multi_select",
    "singleCollaborator": "rich_text",
    "multipleCollaborators": "rich_text",
    "createdBy": "rich_text",
    "lastModifiedBy": "rich_text",
    "multipleAttachments": "rich_text",   # URLs as text (see module docstring)
    "barcode": "rich_text",
    "multipleLookupValues": "rich_text",
    "aiText": "rich_text",
    "externalSyncSource": "rich_text",
}
LINK_TYPE = "multipleRecordLinks"
SKIP_TYPES = {"button"}


@dataclass
class FieldPlan:
    field_id: str
    airtable_type: str
    name: str
    notion_kind: str
    options: set[str] = field(default_factory=set)
    is_title: bool = False
    link_target: str | None = None   # target table id for relation fields


def resolve_kind(f: dict) -> str:
    """Map an Airtable field to a Notion property kind, honoring formula/rollup
    result types so numeric/date computed fields keep their type."""
    ftype = f["type"]
    if ftype in ("formula", "rollup"):
        rtype = ((f.get("options") or {}).get("result") or {}).get("type")
        return {
            "number": "number",
            "currency": "number",
            "percent": "number",
            "date": "date",
            "dateTime": "date",
            "checkbox": "checkbox",
        }.get(rtype, "rich_text")
    return SCALAR_MAP.get(ftype, "rich_text")


def notion_property_def(plan: FieldPlan) -> dict:
    kind = plan.notion_kind
    if plan.is_title:
        return {"title": {}}
    if kind == "number":
        fmt = {"currency": "dollar", "percent": "percent"}.get(plan.airtable_type, "number")
        return {"number": {"format": fmt}}
    if kind in ("select", "multi_select"):
        opts = [{"name": o} for o in sorted(plan.options)][:MAX_SELECT_OPTIONS]
        return {kind: {"options": opts}}
    if kind in ("email", "url", "phone_number", "checkbox", "date", "rich_text"):
        return {kind: {}}
    return {"rich_text": {}}


def notion_property_value(plan: FieldPlan, raw: Any) -> dict | None:
    kind = "title" if plan.is_title else plan.notion_kind

    if raw is None:
        # Notion requires the title property to be present even when empty.
        return {"title": [{"text": {"content": ""}}]} if kind == "title" else None

    flat = flatten(raw)

    if kind == "title":
        return {"title": [{"text": {"content": to_text(flat)[:TEXT_LIMIT]}}]}
    if kind == "rich_text":
        text = to_text(flat)
        return {"rich_text": [{"text": {"content": text[:TEXT_LIMIT]}}]} if text else None
    if kind == "number":
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return {"number": float(raw)}
        try:
            s = str(flat).replace("$", "").replace("%", "").replace(",", "").strip()
            return {"number": float(s)} if s not in ("", "None") else None
        except (TypeError, ValueError):
            return None
    if kind == "checkbox":
        return {"checkbox": bool(raw)}
    if kind == "date":
        s = to_text(flat).strip()
        if not s:
            return None
        return {"date": {"start": s.split(" - ")[0].strip()}}
    if kind == "select":
        name = sanitize_option(to_text(flat))
        return {"select": {"name": name}} if name else None
    if kind == "multi_select":
        values = flat if isinstance(flat, list) else [flat]
        names = [sanitize_option(str(v)) for v in values if str(v).strip()]
        names = [n for n in names if n]
        return {"multi_select": [{"name": n} for n in names]} if names else None
    if kind in ("email", "url", "phone_number"):
        text = to_text(flat).strip()
        return {kind: text} if text else None
    return None


# --------------------------------------------------------------------------- #
# Planning                                                                     #
# --------------------------------------------------------------------------- #

def build_field_plans(
    fields: list[dict], primary_field_id: str
) -> tuple[list[FieldPlan], list[FieldPlan]]:
    """Return (scalar_plans, relation_plans) from an Airtable table's schema."""
    scalar_plans: list[FieldPlan] = []
    relation_plans: list[FieldPlan] = []
    used_names: set[str] = set()

    def unique(name: str) -> str:
        base = (name or "Untitled").strip()[:TEXT_LIMIT] or "Untitled"
        candidate, i = base, 2
        while candidate in used_names:
            candidate = f"{base} ({i})"
            i += 1
        used_names.add(candidate)
        return candidate

    for f in fields:
        fid, ftype = f["id"], f["type"]
        name = unique(f.get("name", "Untitled"))

        # The primary field becomes the Notion title, whatever its Airtable type.
        if fid == primary_field_id:
            scalar_plans.append(FieldPlan(fid, ftype, name, "title", is_title=True))
            continue

        if ftype in SKIP_TYPES:
            log.info("  skipping field %r (type %s has no Notion equivalent)", name, ftype)
            continue

        if ftype == LINK_TYPE:
            target = (f.get("options") or {}).get("linkedTableId")
            relation_plans.append(
                FieldPlan(fid, ftype, name, "relation", link_target=target)
            )
            continue

        kind = resolve_kind(f)
        plan = FieldPlan(fid, ftype, name, kind)

        # Select options come straight from the schema (no data scan needed).
        if kind in ("select", "multi_select"):
            for choice in ((f.get("options") or {}).get("choices") or []):
                nm = sanitize_option(choice.get("name", ""))
                if nm:
                    plan.options.add(nm)

        scalar_plans.append(plan)

    # Airtable always has a primary field, so a title is guaranteed; this is a
    # safety net only.
    if not any(p.is_title for p in scalar_plans) and scalar_plans:
        scalar_plans[0].is_title = True
        scalar_plans[0].notion_kind = "title"

    return scalar_plans, relation_plans


# --------------------------------------------------------------------------- #
# Config + resumable state                                                    #
# --------------------------------------------------------------------------- #

def _split_names(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [name.strip() for name in raw.split(",") if name.strip()]


def load_config() -> dict:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    cfg = {
        "airtable_token": os.environ.get("AIRTABLE_API_TOKEN"),
        "base_id": os.environ.get("AIRTABLE_BASE_ID"),
        "notion_token": os.environ.get("NOTION_API_TOKEN"),
        "parent_page": os.environ.get("NOTION_PARENT_PAGE_ID"),
        "skip_tables": _split_names(os.environ.get("AIRTABLE_SKIP_TABLES")),
        "only_tables": _split_names(os.environ.get("AIRTABLE_ONLY_TABLES")),
    }
    missing = [name for name, key in (
        ("AIRTABLE_API_TOKEN", "airtable_token"),
        ("AIRTABLE_BASE_ID", "base_id"),
        ("NOTION_API_TOKEN", "notion_token"),
        ("NOTION_PARENT_PAGE_ID", "parent_page"),
    ) if not cfg[key]]
    if missing:
        log.error("Missing required environment variable(s): %s", ", ".join(missing))
        log.error("Set them in your shell or a .env file (see .env.example).")
        sys.exit(1)
    return cfg


def state_path_for(base_id: str) -> str:
    safe = "".join(c if (c.isalnum() or c in "-_") else "_" for c in base_id)
    return f"migration_state_{safe}.json"


def load_state(path: str, base_id: str, parent_page: str) -> dict:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            state = json.load(fh)
        if state.get("base_id") == base_id:
            state.setdefault("tables", {})
            done = sum(1 for r in state["tables"].values() if r.get("complete"))
            log.info("Resuming from %s: %d table(s) already complete", path, done)
            return state
        log.warning("State file %s is for a different base; ignoring it.", path)
    return {"base_id": base_id, "parent_page": parent_page, "tables": {}}


def save_state(path: str, state: dict) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, ensure_ascii=False)


# --------------------------------------------------------------------------- #
# Migration driver                                                            #
# --------------------------------------------------------------------------- #

def migrate(
    restart: bool = False,
    skip_tables: list[str] | None = None,
    only_tables: list[str] | None = None,
) -> None:
    cfg = load_config()
    base_id = cfg["base_id"]
    parent_page = cfg["parent_page"]

    skip = set(skip_tables if skip_tables is not None else cfg["skip_tables"])
    only = set(only_tables if only_tables is not None else cfg["only_tables"])
    if skip:
        log.info("Skipping tables: %s", ", ".join(sorted(skip)))
    if only:
        log.info("Migrating only tables: %s", ", ".join(sorted(only)))

    airtable = AirtableClient(cfg["airtable_token"])
    notion = NotionClient(cfg["notion_token"])

    state_path = state_path_for(base_id)
    if restart and os.path.exists(state_path):
        log.warning(
            "--restart: discarding saved progress in %s. Databases already "
            "created in Notion are NOT removed; delete them manually to avoid "
            "duplicates.", state_path,
        )
        os.remove(state_path)

    state = load_state(state_path, base_id, parent_page)
    tables_state: dict[str, dict] = state["tables"]

    tables = airtable.list_tables(base_id)
    log.info("Found %d table(s) in Airtable base %s", len(tables), base_id)

    # -------- Pass 1: schema + records -------- #
    for tbl in tables:
        tid, tname = tbl["id"], tbl.get("name", tbl["id"])

        if only and tname not in only and tid not in tables_state:
            log.info("Skipping table %r: not in --only list", tname)
            continue
        if tname in skip and tid not in tables_state:
            log.info("Skipping table %r: in skip list", tname)
            continue

        rec = tables_state.get(tid)
        if rec and rec.get("complete"):
            log.info("Skipping table %r (%s): already migrated, %d rows",
                     tname, tid, len(rec["row_map"]))
            continue

        log.info("Reading table %r (%s)", tname, tid)
        fields = tbl.get("fields", [])
        primary_field_id = tbl.get("primaryFieldId")
        records = airtable.list_records(base_id, tid)
        log.info("  %d fields, %d records", len(fields), len(records))

        scalar_plans, relation_plans = build_field_plans(fields, primary_field_id)

        if rec and rec.get("notion_data_source_id"):
            ds_id = rec["notion_data_source_id"]
            row_map = rec["row_map"]
            log.info("  resuming into existing database %s (%d/%d rows done)",
                     rec["notion_database_id"], len(row_map), len(records))
        else:
            properties = {p.name: notion_property_def(p) for p in scalar_plans}
            db_id, ds_id = notion.create_database(parent_page, tname, properties)
            row_map = {}
            log.info("  created Notion database %s (data source %s)", db_id, ds_id)
            rec = {
                "airtable_table_id": tid,
                "airtable_table_name": tname,
                "notion_database_id": db_id,
                "notion_data_source_id": ds_id,
                "row_map": row_map,
                "relations": [
                    {"name": rp.name, "field_id": rp.field_id, "link_target": rp.link_target}
                    for rp in relation_plans
                ],
                "complete": False,
                "relations_wired": False,
            }
            tables_state[tid] = rec
            save_state(state_path, state)

        inserted = 0
        for record in records:
            rid = record["id"]
            if rid in row_map:
                continue
            values = record.get("fields", {})
            props: dict[str, Any] = {}
            for p in scalar_plans:
                built = notion_property_value(p, values.get(p.field_id))
                if built is not None:
                    props[p.name] = built
            title_plan = next((p for p in scalar_plans if p.is_title), None)
            if title_plan and title_plan.name not in props:
                props[title_plan.name] = {"title": [{"text": {"content": ""}}]}
            page_id = notion.create_page(ds_id, props)
            row_map[rid] = page_id
            inserted += 1
            if inserted % SAVE_EVERY == 0:
                save_state(state_path, state)

        rec["complete"] = True
        save_state(state_path, state)
        log.info("  inserted %d new page(s); %d rows total", inserted, len(row_map))

    # -------- Pass 2: relations -------- #
    ds_by_table = {tid: rec["notion_data_source_id"] for tid, rec in tables_state.items()}

    for tid, rec in tables_state.items():
        if not rec["relations"]:
            rec["relations_wired"] = True
            continue
        if rec.get("relations_wired"):
            log.info("Skipping relations for %r: already wired", rec["airtable_table_name"])
            continue

        log.info("Wiring relations for table %r", rec["airtable_table_name"])
        records = airtable.list_records(base_id, tid)
        rows_by_id = {r["id"]: r for r in records}

        for rel in rec["relations"]:
            field_id, prop_name, target_tbl = rel["field_id"], rel["name"], rel["link_target"]
            target_ds = ds_by_table.get(target_tbl)
            if target_ds is None:
                log.warning(
                    "  %r links to table %s which was not migrated; skipping",
                    prop_name, target_tbl,
                )
                continue

            notion.add_relation_property(rec["notion_data_source_id"], prop_name, target_ds)

            target_row_map = tables_state[target_tbl]["row_map"]
            wired = 0
            for rid, page_id in rec["row_map"].items():
                row = rows_by_id.get(rid)
                if not row:
                    continue
                linked = row.get("fields", {}).get(field_id) or []
                target_pages = [target_row_map[x] for x in linked if x in target_row_map]
                if target_pages:
                    notion.set_page_relation(page_id, prop_name, target_pages)
                    wired += 1
            log.info("  %r -> %s: linked %d rows", prop_name, target_tbl, wired)

        rec["relations_wired"] = True
        save_state(state_path, state)

    save_state(state_path, state)
    log.info("Done. State saved to %s", state_path)
    print("\nSummary")
    for rec in tables_state.values():
        print(
            f"  {rec['airtable_table_name']}: {len(rec['row_map'])} rows, "
            f"{len(rec['relations'])} relation field(s) -> database {rec['notion_database_id']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate an Airtable base's tables into Notion.")
    parser.add_argument(
        "--restart", action="store_true",
        help="Discard saved progress for this base and start over. Does not "
             "delete databases already created in Notion.",
    )
    parser.add_argument(
        "--skip", metavar="NAMES",
        help="Comma-separated table names to exclude. Overrides AIRTABLE_SKIP_TABLES.",
    )
    parser.add_argument(
        "--only", metavar="NAMES",
        help="Comma-separated table names to migrate exclusively. Overrides "
             "AIRTABLE_ONLY_TABLES.",
    )
    args = parser.parse_args()
    skip = _split_names(args.skip) if args.skip is not None else None
    only = _split_names(args.only) if args.only is not None else None
    try:
        migrate(restart=args.restart, skip_tables=skip, only_tables=only)
    except KeyboardInterrupt:
        log.warning("Interrupted. Progress was saved; re-run to resume.")


if __name__ == "__main__":
    main()
