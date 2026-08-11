import argparse
import psycopg2
from psycopg2.extras import Json

from common import (
    SN_INSTANCE, SN_AUTH, PG_CONFIG,
    extract_ref_value, parse_sn_datetime, utc_now,
    run_extraction,
)

TABLE_NAME = "sys_user_group"
URL = f"{SN_INSTANCE}/api/now/table/sys_user_group"


def upsert_sys_user_group(records):
    """
    Writes extracted assignment group records into raw.sys_user_group.
    Same upsert pattern as incidents — one row per group, always current.
    """
    if not records:
        return 0

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    for r in records:
        cur.execute(
            """
            INSERT INTO raw.sys_user_group (
                sys_id, name, description, active, manager, parent,
                email, sys_created_on, sys_updated_on, raw_payload, _loaded_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sys_id) DO UPDATE SET
                name            = EXCLUDED.name,
                description     = EXCLUDED.description,
                active          = EXCLUDED.active,
                manager         = EXCLUDED.manager,
                parent          = EXCLUDED.parent,
                email           = EXCLUDED.email,
                sys_created_on  = EXCLUDED.sys_created_on,
                sys_updated_on  = EXCLUDED.sys_updated_on,
                raw_payload     = EXCLUDED.raw_payload,
                _loaded_at      = EXCLUDED._loaded_at
            """,
            (
                r["sys_id"],
                r.get("name"),
                r.get("description"),
                r.get("active"),
                extract_ref_value(r.get("manager")),
                extract_ref_value(r.get("parent")),
                r.get("email"),
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
    print(f"Extracting sys_user_group from {SN_INSTANCE}...")

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
        upsert_fn=upsert_sys_user_group,
        full_backfill=args.full_backfill,
    )
