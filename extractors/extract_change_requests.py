import argparse
import psycopg2
from psycopg2.extras import Json

from common import (
    SN_INSTANCE, SN_AUTH, PG_CONFIG,
    extract_ref_value, parse_sn_datetime, utc_now,
    run_extraction,
)

TABLE_NAME = "change_request"
URL = f"{SN_INSTANCE}/api/now/table/change_request"


def upsert_change_requests(records):
    """
    Writes extracted change request records into raw.change_requests.
    Same upsert pattern as incidents — one row per change, always current
    state. Mirrors raw.incidents in shape since change_request is
    structurally very similar (its own fact table for a different ITSM
    process, referencing the same assignment_group/cmdb_ci dimensions).
    """
    if not records:
        return 0

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    for r in records:
        cur.execute(
            """
            INSERT INTO raw.change_requests (
                sys_id, number, short_description, type, risk, priority,
                state, approval, assignment_group, assigned_to, cmdb_ci,
                requested_by, start_date, end_date, sys_created_on,
                sys_updated_on, raw_payload, _loaded_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sys_id) DO UPDATE SET
                number              = EXCLUDED.number,
                short_description   = EXCLUDED.short_description,
                type                = EXCLUDED.type,
                risk                = EXCLUDED.risk,
                priority            = EXCLUDED.priority,
                state               = EXCLUDED.state,
                approval            = EXCLUDED.approval,
                assignment_group    = EXCLUDED.assignment_group,
                assigned_to         = EXCLUDED.assigned_to,
                cmdb_ci             = EXCLUDED.cmdb_ci,
                requested_by        = EXCLUDED.requested_by,
                start_date          = EXCLUDED.start_date,
                end_date            = EXCLUDED.end_date,
                sys_created_on      = EXCLUDED.sys_created_on,
                sys_updated_on      = EXCLUDED.sys_updated_on,
                raw_payload         = EXCLUDED.raw_payload,
                _loaded_at          = EXCLUDED._loaded_at
            """,
            (
                r["sys_id"],
                r.get("number"),
                r.get("short_description"),
                r.get("type"),
                r.get("risk"),
                r.get("priority"),
                r.get("state"),
                r.get("approval"),
                extract_ref_value(r.get("assignment_group")),
                extract_ref_value(r.get("assigned_to")),
                extract_ref_value(r.get("cmdb_ci")),
                extract_ref_value(r.get("requested_by")),
                parse_sn_datetime(r.get("start_date")),
                parse_sn_datetime(r.get("end_date")),
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


if __name__ == "__main__":
    print(f"Extracting change_request from {SN_INSTANCE}...")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-backfill",
        action="store_true",
        help="Ignore the stored watermark and pull all records (one-time historical load).",
    )
    args = parser.parse_args()

    run_extraction(
        table_name=TABLE_NAME,
        url=URL,
        auth=SN_AUTH,
        upsert_fn=upsert_change_requests,
        full_backfill=args.full_backfill,
    )
