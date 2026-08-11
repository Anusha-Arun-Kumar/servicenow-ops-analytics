import argparse
import psycopg2
from psycopg2.extras import Json

from common import (
    SN_INSTANCE, SN_AUTH, PG_CONFIG,
    extract_ref_value, parse_sn_datetime, utc_now,
    run_extraction,
)

TABLE_NAME = "cmdb_ci"
URL = f"{SN_INSTANCE}/api/now/table/cmdb_ci"


def upsert_cmdb_ci(records):
    """
    Writes extracted configuration item records into raw.cmdb_ci.
    Same upsert pattern as the other reference tables — one row per CI,
    always current.
    """
    if not records:
        return 0

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    for r in records:
        cur.execute(
            """
            INSERT INTO raw.cmdb_ci (
                sys_id, name, sys_class_name, operational_status,
                install_status, category, subcategory, assigned_to,
                support_group, sys_created_on, sys_updated_on,
                raw_payload, _loaded_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sys_id) DO UPDATE SET
                name                = EXCLUDED.name,
                sys_class_name      = EXCLUDED.sys_class_name,
                operational_status  = EXCLUDED.operational_status,
                install_status      = EXCLUDED.install_status,
                category            = EXCLUDED.category,
                subcategory         = EXCLUDED.subcategory,
                assigned_to         = EXCLUDED.assigned_to,
                support_group       = EXCLUDED.support_group,
                sys_created_on      = EXCLUDED.sys_created_on,
                sys_updated_on      = EXCLUDED.sys_updated_on,
                raw_payload         = EXCLUDED.raw_payload,
                _loaded_at          = EXCLUDED._loaded_at
            """,
            (
                r["sys_id"],
                r.get("name"),
                r.get("sys_class_name"),
                r.get("operational_status"),
                r.get("install_status"),
                r.get("category"),
                r.get("subcategory"),
                extract_ref_value(r.get("assigned_to")),
                extract_ref_value(r.get("support_group")),
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
    print(f"Extracting cmdb_ci from {SN_INSTANCE}...")

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
        upsert_fn=upsert_cmdb_ci,
        full_backfill=args.full_backfill,
    )
