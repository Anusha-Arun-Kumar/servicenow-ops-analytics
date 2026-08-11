"""
extractors/common.py

Shared logic used by every table-specific ServiceNow extractor:
pagination, retry handling, watermark tracking, extraction logging,
and the generic run_extraction() orchestrator that ties them together.

Table-specific scripts (extract_incidents.py, extract_change_requests.py,
etc.) import from here and only define what's actually unique to that
table: the URL and the upsert function for its target Postgres table.
"""

import os
import requests
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, wait_exponential, stop_after_attempt

load_dotenv()

# --- ServiceNow connection settings ---
SN_INSTANCE = os.getenv("SERVICENOW_INSTANCE")
SN_USER = os.getenv("SERVICENOW_USER")
SN_PASSWORD = os.getenv("SERVICENOW_PASSWORD")

if not all([SN_INSTANCE, SN_USER, SN_PASSWORD]):
    raise ValueError("Missing one or more ServiceNow env vars — check your .env file")

SN_AUTH = (SN_USER, SN_PASSWORD)

# --- Postgres connection settings ---
PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "servicenow_dw"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

DEFAULT_LOOKBACK_DAYS = 90  # first-ever run for a table: only pull last N days, not full history


def utc_now():
    """
    Timezone-aware UTC now, stripped back to naive for compatibility with
    our Postgres TIMESTAMP (not TIMESTAMPTZ) columns. Replaces the
    deprecated datetime.utcnow().
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_sn_datetime(value):
    """ServiceNow datetime strings ('' or 'YYYY-MM-DD HH:MM:SS') -> datetime or None."""
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def extract_ref_value(field):
    """
    ServiceNow reference fields (e.g. assignment_group, assigned_to) come
    back as {"link": "...", "value": "sys_id"} when populated, or as an
    empty string "" when not set. This normalizes both cases so we always
    store either a clean sys_id string or None.
    """
    if isinstance(field, dict):
        return field.get("value") or None
    return field or None


# --- Watermark / extraction_log helpers ---

def get_last_watermark(table_name):
    """
    Looks up the most recent successful watermark for this table from
    raw.extraction_log. If none exists yet (first run ever), falls back
    to a bounded lookback window instead of pulling all history.
    """
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT new_watermark
        FROM raw.extraction_log
        WHERE table_name = %s AND status = 'success'
        ORDER BY new_watermark DESC
        LIMIT 1
        """,
        (table_name,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row and row[0]:
        return row[0]
    else:
        fallback = utc_now() - timedelta(days=DEFAULT_LOOKBACK_DAYS)
        print(f"  No prior watermark found for '{table_name}' — using {DEFAULT_LOOKBACK_DAYS}-day lookback: {fallback}")
        return fallback


def start_extraction_log(table_name, watermark_used):
    """Inserts a 'running' row and returns its id so we can update it later."""
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO raw.extraction_log
            (table_name, extraction_started_at, watermark_used, status)
        VALUES (%s, %s, %s, 'running')
        RETURNING id
        """,
        (table_name, utc_now(), watermark_used),
    )
    log_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return log_id


def complete_extraction_log(log_id, new_watermark, records_extracted, status="success"):
    """Updates the log row once extraction finishes (success or failed)."""
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE raw.extraction_log
        SET extraction_completed_at = %s,
            new_watermark = %s,
            records_extracted = %s,
            status = %s
        WHERE id = %s
        """,
        (utc_now(), new_watermark, records_extracted, status, log_id),
    )
    conn.commit()
    cur.close()
    conn.close()


# --- Pagination + retry ---

class NonRetryableError(Exception):
    """Raised for client errors (4xx) that retrying won't fix — e.g. bad auth, bad query syntax."""
    pass


@retry(
    retry=retry_if_exception_type(
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError)
    ),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    stop=stop_after_attempt(4),
    reraise=True,
)
def fetch_page(url, auth, page_params):
    """
    Fetches a single page from the API, with automatic retries on
    transient failures only: timeouts, connection errors, and 5xx
    server errors. 4xx client errors (bad auth, bad query) are raised
    as NonRetryableError instead — tenacity won't retry that type, so
    those fail immediately and loudly, since retrying them is pointless.
    """
    response = requests.get(
        url,
        auth=auth,
        headers={"Accept": "application/json"},
        params=page_params,
    )

    if 500 <= response.status_code < 600:
        response.raise_for_status()  # -> requests.exceptions.HTTPError, WILL be retried
    elif response.status_code >= 400:
        raise NonRetryableError(f"HTTP {response.status_code}: {response.text[:200]}")  # will NOT be retried

    try:
        return response.json()["result"]
    except requests.exceptions.JSONDecodeError:
        # A 200 status with a non-JSON body usually means the ServiceNow
        # instance itself isn't actually serving the API right now — most
        # commonly because a Personal Developer Instance has hibernated
        # after a period of inactivity. Surface that clearly instead of
        # letting a raw JSONDecodeError leak out looking like a code bug.
        if "hibernat" in response.text.lower():
            raise NonRetryableError(
                "ServiceNow instance appears to be HIBERNATING. "
                "Go to developer.servicenow.com and wake it up, then retry."
            )
        raise NonRetryableError(f"Expected JSON but got non-JSON response: {response.text[:200]}")


def fetch_all_records(url, auth, base_params, page_size=100):
    """
    Pulls ALL records from a ServiceNow table endpoint, handling pagination.
    Keeps requesting pages until a page comes back empty — that's the
    signal we've reached the last page (safe even under ACL-filtered
    short pages — see Phase 1 docs for the full explanation).
    """
    all_records = []
    offset = 0

    while True:
        page_params = {
            **base_params,
            "sysparm_limit": page_size,
            "sysparm_offset": offset,
        }

        page_data = fetch_page(url, auth, page_params)
        all_records.extend(page_data)

        print(f"  Fetched page at offset {offset}: {len(page_data)} records")

        if len(page_data) == 0:
            break

        offset += page_size

    return all_records


# --- Generic orchestrator ---

def run_extraction(table_name, url, auth, upsert_fn, full_backfill=False, page_size=100):
    """
    The full extraction flow, generic across every ServiceNow table:
      1. Resolve the watermark (or override for full backfill)
      2. Log the run as 'running'
      3. Fetch all new/changed records since the watermark
      4. Upsert them via the table-specific upsert_fn
      5. Save the new watermark and mark the run 'success'
      (or mark 'failed' and re-raise if anything above breaks)

    Table-specific scripts plug in their own `url` and `upsert_fn` —
    everything else is identical across tables.
    """
    if full_backfill:
        watermark = datetime(2000, 1, 1)
        print(f"Running FULL BACKFILL for '{table_name}' — ignoring stored watermark")
    else:
        watermark = get_last_watermark(table_name)

    watermark_str = watermark.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Using watermark: {watermark_str}")

    log_id = start_extraction_log(table_name, watermark)

    try:
        base_params = {
            "sysparm_query": f"sys_updated_on>{watermark_str}",
            "sysparm_order_by": "sys_updated_on",
        }

        records = fetch_all_records(url=url, auth=auth, base_params=base_params, page_size=page_size)
        print(f"\nTotal records extracted: {len(records)}")

        written = upsert_fn(records)
        print(f"Upserted {written} records into raw.{table_name}")

        if records:
            max_seen = max(parse_sn_datetime(r["sys_updated_on"]) for r in records)
            now = utc_now()
            if max_seen > now:
                print(
                    f"  WARNING: source data contains a future-dated sys_updated_on "
                    f"({max_seen}) — clamping watermark to current time ({now}) "
                    f"instead, to avoid silently skipping real future changes."
                )
            new_watermark = min(max_seen, now).strftime("%Y-%m-%d %H:%M:%S")
        else:
            new_watermark = watermark_str
        complete_extraction_log(log_id, new_watermark, len(records), status="success")
        print(f"New watermark saved: {new_watermark}")

        return records

    except Exception:
        complete_extraction_log(log_id, None, 0, status="failed")
        raise