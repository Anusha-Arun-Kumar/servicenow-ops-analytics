import os
import argparse
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception_type, wait_exponential, stop_after_attempt


def utc_now():
    """
    Timezone-aware UTC now, stripped back to naive for compatibility with
    our Postgres TIMESTAMP (not TIMESTAMPTZ) columns. Replaces the
    deprecated datetime.utcnow().
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)

load_dotenv()

instance = os.getenv("SERVICENOW_INSTANCE")
user = os.getenv("SERVICENOW_USER")
password = os.getenv("SERVICENOW_PASSWORD")

if not all([instance, user, password]):
    raise ValueError("Missing one or more ServiceNow env vars — check your .env file")

url = f"{instance}/api/now/table/incident"
auth = (user, password)

# --- Postgres connection settings ---
# These default to your local docker-compose values, but can be overridden
# via .env if you ever point this at a different Postgres instance.
PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "servicenow_dw"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

DEFAULT_LOOKBACK_DAYS = 90  # first-ever run: only pull last 90 days, not full history
TABLE_NAME = "incident"


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
        print(f"  No prior watermark found — using {DEFAULT_LOOKBACK_DAYS}-day lookback: {fallback}")
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

    return response.json()["result"]


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


def parse_sn_datetime(value):
    """ServiceNow datetime strings ('' or 'YYYY-MM-DD HH:MM:SS') -> datetime or None."""
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def upsert_incidents(records):
    """
    Writes extracted incident records into raw.incidents.
    Uses INSERT ... ON CONFLICT (sys_id) DO UPDATE — this is the upsert
    pattern: new incidents get inserted, previously-seen incidents (same
    sys_id) get their row overwritten with the latest data. This keeps
    raw.incidents as a current-state table, one row per incident, as
    opposed to a full change-history table (a separate, future addition).
    """
    if not records:
        return 0

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    for r in records:
        cur.execute(
            """
            INSERT INTO raw.incidents (
                sys_id, number, short_description, priority, state,
                assignment_group, assigned_to, opened_at, closed_at,
                sys_created_on, sys_updated_on, raw_payload, _loaded_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sys_id) DO UPDATE SET
                number             = EXCLUDED.number,
                short_description  = EXCLUDED.short_description,
                priority            = EXCLUDED.priority,
                state               = EXCLUDED.state,
                assignment_group    = EXCLUDED.assignment_group,
                assigned_to         = EXCLUDED.assigned_to,
                opened_at           = EXCLUDED.opened_at,
                closed_at           = EXCLUDED.closed_at,
                sys_created_on      = EXCLUDED.sys_created_on,
                sys_updated_on      = EXCLUDED.sys_updated_on,
                raw_payload         = EXCLUDED.raw_payload,
                _loaded_at          = EXCLUDED._loaded_at
            """,
            (
                r["sys_id"],
                r.get("number"),
                r.get("short_description"),
                r.get("priority"),
                r.get("state"),
                extract_ref_value(r.get("assignment_group")),
                extract_ref_value(r.get("assigned_to")),
                parse_sn_datetime(r.get("opened_at")),
                parse_sn_datetime(r.get("closed_at")),
                parse_sn_datetime(r.get("sys_created_on")),
                parse_sn_datetime(r.get("sys_updated_on")),
                Json(r),
                utc_now(),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    return len(records)


def fetch_all_records(url, auth, base_params, page_size=100):
    """
    Pulls ALL records from a ServiceNow table endpoint, handling pagination.
    Keeps requesting pages until a page comes back empty — that's the
    signal we've reached the last page (see ACL note in Phase 1 docs for
    why we don't stop on a merely *short* page).
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
            break  # truly no more records — safe stopping point even if
                    # ACL-filtered pages came back short earlier

        offset += page_size

    return all_records


if __name__ == "__main__":
    print(f"Extracting incidents from {instance}...")

    # 0. Parse CLI args — supports normal incremental runs, or a one-time
    #    full backfill that ignores the watermark and pulls everything.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-backfill",
        action="store_true",
        help="Ignore the stored watermark and pull all records (one-time historical load).",
    )
    args = parser.parse_args()

    # 1. Figure out where we left off last time (or the lookback default),
    #    UNLESS --full-backfill was passed, in which case we deliberately
    #    use a far-past date so the sys_updated_on filter matches everything.
    if args.full_backfill:
        watermark = datetime(2000, 1, 1)
        print("Running FULL BACKFILL — ignoring stored watermark, pulling all records")
    else:
        watermark = get_last_watermark(TABLE_NAME)

    watermark_str = watermark.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Using watermark: {watermark_str}")

    # 2. Log that a run is starting
    log_id = start_extraction_log(TABLE_NAME, watermark)

    try:
        # 3. Only ask ServiceNow for records updated since the watermark
        base_params = {
            "sysparm_query": f"sys_updated_on>{watermark_str}",
            "sysparm_order_by": "sys_updated_on",  # helps us reliably find the max value
        }

        records = fetch_all_records(
            url=url,
            auth=auth,
            base_params=base_params,
            page_size=100,
        )

        print(f"\nTotal records extracted: {len(records)}")

        # 4. Write extracted records into raw.incidents (upsert)
        written = upsert_incidents(records)
        print(f"Upserted {written} records into raw.incidents")

        # 5. Compute the new watermark = latest sys_updated_on we actually saw
        if records:
            new_watermark = max(r["sys_updated_on"] for r in records)
        else:
            new_watermark = watermark_str  # nothing new — watermark doesn't move

        complete_extraction_log(log_id, new_watermark, len(records), status="success")
        print(f"New watermark saved: {new_watermark}")

    except Exception as e:
        # If anything above fails, mark this run as failed rather than leaving
        # it stuck as 'running' or (worse) silently logging a bad watermark
        complete_extraction_log(log_id, None, 0, status="failed")
        raise