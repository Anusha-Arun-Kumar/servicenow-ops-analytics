# Phase 1 Recap: Extraction

## What Phase 1 accomplished
Turned `extractors/extract_incidents.py` from a "prints 10 records" connectivity test into a production-style extractor: paginated, incremental, retry-resilient, and writing clean upserted data into Postgres — with both a scheduled incremental mode and a manual full-backfill mode.

---

## Step 1: Pagination
- ServiceNow's Table API caps records per request (`sysparm_limit`); real tables can have thousands of rows
- Built `fetch_all_records()` — loops through pages using `sysparm_offset`, accumulating results
- Initial stopping condition: "page returned fewer records than `page_size` = last page"
- Verified with a real run: 67 incidents across 7 pages (6 full pages of 10, 1 partial page of 7) when testing with `page_size=10`

**Design decision — separating pagination from filtering:**
- `fetch_all_records(url, auth, base_params, page_size)` takes filters as a parameter (`base_params`) rather than hardcoding them
- This meant Step 3 (incremental filtering) required zero changes to the pagination logic itself — just a different `base_params` value passed in

## Step 2: The ACL short-page edge case
- Raised a real scenario from production ServiceNow experience: **scripted ACLs** can strip rows from a page *after* the underlying query already ran with a full `limit`, meaning a page can come back short even when more data exists at the next offset
- This breaks the naive "short page = last page" assumption, because offset-based pagination assumes page size directly reflects remaining data
- **Fix applied:** changed the stopping condition to "page returned *zero* records" instead of "fewer than `page_size`" — costs one extra empty API call at the very end, but is safe regardless of ACL filtering
- **Documented alternative (not implemented, noted as future improvement):** cursor/keyset pagination — paginate by `sys_id > last_seen_sys_id` instead of numeric offset. Immune to both the ACL issue and to record drift caused by concurrent inserts/updates during pagination (offsets can shift under a live table; a cursor anchored to actual data can't)

## Step 3: Incremental extraction with watermarking
**Why:** re-pulling the entire table on every run doesn't scale — wasteful, slow, and risks API rate limits. Incremental extraction only pulls records changed since the last run.

**Watermark = the most recent `sys_updated_on` value successfully pulled.** Stored in a dedicated tracking table rather than a local file, so it lives next to the data it describes and survives container rebuilds.

**`raw.extraction_log` schema:**
```sql
CREATE TABLE raw.extraction_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    extraction_started_at TIMESTAMP NOT NULL,
    extraction_completed_at TIMESTAMP,
    watermark_used TIMESTAMP,
    new_watermark TIMESTAMP,
    records_extracted INTEGER,
    status VARCHAR(20) NOT NULL DEFAULT 'running'
);
```
- `status` matters: `get_last_watermark()` only trusts rows where `status = 'success'` — a failed run's watermark is never used, protecting future runs from building on bad data
- Logged as `'running'` *before* the API call, updated after — so a mid-run crash leaves a visible orphaned `'running'` row (a debugging breadcrumb) rather than silent failure

**First-run design (bounded lookback, user's own reasoning):**
- A true first run has no prior watermark. Full historical pull is one option but can be slow/unbounded for large tables.
- Landed on a **bounded lookback window** (default 90 days) as the first-run fallback — a business-justified default (recent data matters most for operational dashboards) rather than "everything" or "nothing"
- Implemented via `DEFAULT_LOOKBACK_DAYS = 90` and a fallback branch in `get_last_watermark()`

**Verified end-to-end:** manually edited one incident in the ServiceNow UI, re-ran the script, confirmed exactly 1 record came back — proof the `sys_updated_on` filter and watermark tracking work correctly together.

## Step 4: Retry logic with `tenacity`
**Why:** not all API failures are equal. Transient failures (timeouts, network blips, 5xx server errors) often succeed on retry. Permanent failures (bad auth, malformed query — 4xx) will never succeed no matter how many times you retry, and blindly retrying them wastes time and can mask real bugs.

**Design:**
- `fetch_page()` (single-page request, isolated from the pagination loop so retries don't restart already-fetched pages) wrapped in `@retry`
- Retries only on `Timeout`, `ConnectionError`, and `HTTPError` (raised for 5xx responses)
- 4xx responses raise a custom `NonRetryableError` instead — a type deliberately excluded from the retry list, so it propagates immediately
- Exponential backoff: `wait_exponential(multiplier=2, min=2, max=30)`, capped at 4 total attempts

**Verified both paths concretely:**
- Deliberately broke `.env` password → failed in under a second with `NonRetryableError: HTTP 401`, no retry delay — confirmed 4xx short-circuits correctly
- Restored password → normal run resumed cleanly

## Step 5: Writing to Postgres (`raw.incidents`)
**Design decision — upsert (SCD Type 1) vs. full history (SCD Type 2), user's own reasoning:**
- Concluded `raw.incidents` should hold one row per incident, always reflecting current state — dashboards asking "how many P1s are open right now" want latest truth, not a history to reconcile
- Separately identified that change-history questions (e.g. "how many times did this incident's assignment group change?") need a *different* table — one row per version, timestamped
- Decision: build the upsert table now (feeds Phase 2 dbt marts directly); documented the history table as a future addition rather than building both immediately

**Schema:**
```sql
CREATE TABLE raw.incidents (
    sys_id             VARCHAR(32) PRIMARY KEY,
    number             VARCHAR(20),
    short_description  TEXT,
    priority           VARCHAR(10),
    state              VARCHAR(10),
    assignment_group   VARCHAR(32),
    assigned_to        VARCHAR(32),
    opened_at          TIMESTAMP,
    closed_at          TIMESTAMP,
    sys_created_on     TIMESTAMP,
    sys_updated_on     TIMESTAMP NOT NULL,
    raw_payload        JSONB NOT NULL,
    _loaded_at         TIMESTAMP NOT NULL DEFAULT now()
);
```
- `sys_id` as primary key is what makes upserting possible (`INSERT ... ON CONFLICT (sys_id) DO UPDATE`)
- `raw_payload JSONB` keeps the full original API response — nothing lost if a field needed later in dbt wasn't extracted into its own column
- Reference fields (`assignment_group`, `assigned_to`) come back from the API as `{"link": ..., "value": "<sys_id>"}` — `extract_ref_value()` normalizes this to a plain sys_id string or `None`

**Verified the update half of upsert specifically** (not just insert): edited the same incident twice, confirmed the row count for that `sys_id` stayed at 1 while `sys_updated_on`/`_loaded_at` changed — proof `ON CONFLICT DO UPDATE` overwrites rather than duplicates.

## Full backfill mode
**The gap it solves:** incremental extraction assumes a prior full load exists to build on. Since the Postgres-write capability was added *after* the watermark was already caught up (from earlier console-only testing), `raw.incidents` only ever received the 1-2 records touched during later testing — not the full original 67. A reset-the-watermark approach (`DELETE FROM raw.extraction_log`) would only backfill the last 90 days, missing older seed data.

**Solution:** a `--full-backfill` CLI flag (via `argparse`) that overrides the watermark with a far-past date (`2000-01-01`), forcing the `sys_updated_on` filter to match everything, while every other step (logging, upsert, watermark update) runs identically to a normal run.

**How the flag decision actually works — no persistent "backfill mode" state:**
- `argparse` reads whatever was typed after `python extract_incidents.py` **fresh, on every single run** — there's no memory of past runs baked into the flag itself
- No flag → `args.full_backfill = False` → normal `get_last_watermark()` path
- `--full-backfill` present → `args.full_backfill = True` → hardcoded year-2000 watermark
- It's "one-time" only by *convention* (you choose when to type it), not because the code tracks having done it before

**How this fits with the future Airflow scheduler (Phase 3):**
- The scheduled DAG task will always invoke the script **without** `--full-backfill` — every automated run, forever, does normal incremental extraction
- `--full-backfill` exists purely as a manual escape hatch for humans — used rarely (e.g. after wiping the database, or onboarding a brand-new table for the first time)
- This is a standard real-world pattern: dbt itself has an equivalent `--full-refresh` flag for the same reason — a deliberate manual override sitting alongside the default scheduled behavior

---

## Architecture at the end of Phase 1

```
ServiceNow Dev Instance
        |
        | GET /api/now/table/incident (paginated, incremental filter, retried)
        v
extract_incidents.py
        |
        |---> raw.extraction_log   (watermark tracking, run history, success/fail status)
        |
        |---> raw.incidents        (upserted, one row per incident, current state)
                    ^
                    |
              Ready for Phase 2 (dbt staging/intermediate/marts)
```

## Good to knows
- **Offset-based pagination has two distinct failure modes**, not just one: (1) scripted ACLs can shrink a page after the query already ran, breaking "short page = done"; (2) concurrent writes to a live table can shift what's "at" a given offset between requests. Both are solved by different techniques — (1) by "stop only on empty page," (2) by cursor/keyset pagination — worth being able to name both even if only the first was implemented here.
- **`sysparm_order_by` was added alongside the incremental filter** so `max(sys_updated_on)` on the results is meaningful and results are returned in a predictable order — not strictly required for correctness here, but good practice.
- **`datetime.utcnow()` is deprecated in modern Python** (3.12+) in favor of timezone-aware datetimes. Fixed via a small `utc_now()` helper that builds a timezone-aware datetime then strips the tzinfo — necessary because the Postgres columns are plain `TIMESTAMP` (not `TIMESTAMPTZ`), and mixing aware/naive datetimes against a naive column raises errors. Keeping everything naive-but-UTC consistently (matching ServiceNow's own naive UTC timestamps) avoided a schema change.
- **A real debugging lesson from this phase:** when a code change didn't take effect (page_size edit "not working"), the actual bug was that the same value was set in two places (function default vs. call-site argument) — only one of which had been edited. General lesson: when an edit seems to have no effect, grep for every place that value could be set before assuming the logic itself is wrong.
- **A second real lesson:** a careless file edit (inserting new functions) accidentally deleted an existing function's `def` line, leaving its body orphaned under the wrong function. Caught immediately by running `python -m py_compile` and `grep -n "^def "` before re-running — a cheap sanity check worth doing any time code is inserted near existing function boundaries.
- **CLI flags are stateless** — `argparse` just reads whatever's typed at invocation time; there's no built-in "remember this was a one-time backfill" behavior. The incremental-vs-backfill decision lives in which command a human (or later, Airflow's DAG definition) chooses to run, not in any stored state.

## Interview Q&A

**Q: How did you handle pagination against an API that doesn't return a total record count upfront?**
A: I looped on `sysparm_offset`, requesting successive pages, and used an empty page as the termination signal rather than a partial one — since a page can come back shorter than requested even when more data exists, if row-level security (ACLs) strips records after the underlying query already ran with a full limit.

**Q: How do you decide what to retry on API failures versus what to fail immediately?**
A: I separated transient failures (timeouts, connection errors, 5xx server errors) from permanent ones (4xx client errors like bad auth). Only the transient category gets retried, with exponential backoff via `tenacity`. Blindly retrying a 401 wastes time and can hide a real configuration bug, so those raise a custom exception type that's deliberately excluded from the retry policy and fails fast.

**Q: How does your extractor avoid re-pulling the entire source table on every run?**
A: I track a watermark — the max `sys_updated_on` seen in the last successful run — in a dedicated Postgres logging table, and filter each new request to only records updated after that watermark. The logging table also tracks run status, so a failed run's watermark is never trusted by the next run.

**Q: How did you handle the very first extraction run, when there's no prior watermark yet?**
A: I used a bounded lookback window (90 days) rather than either extreme — pulling the full history unconditionally, or requiring a manually-set starting point. It's a business-justified default: for ops/operational dashboards, very old records matter less than the last few months, and it keeps first-run time and API load predictable.

**Q: How do you keep a "current state" table from accumulating duplicate rows as source records get updated?**
A: I used an upsert pattern — `INSERT ... ON CONFLICT (primary_key) DO UPDATE` — keyed on the source system's unique ID. Each source record maps to exactly one row, always reflecting its latest state. If change history were needed instead (e.g. "how many times did this record's field X change"), that's a different, append-only table design — effectively SCD Type 1 (overwrite) versus SCD Type 2 (versioned history), and the two serve different query needs.

**Q: If your pipeline runs on a schedule, how do you handle needing a one-time full historical load?**
A: I built a `--full-backfill` CLI flag that overrides the stored watermark with a far-past date, so the same extraction logic pulls everything instead of just recent changes. The scheduled/automated runs never pass that flag — it's a manual override for rare cases like disaster recovery or onboarding a new table, not part of the regular schedule. It's the same pattern dbt uses with its own `--full-refresh` flag.

**Q: Why store the full raw API response (JSONB) alongside typed columns, instead of just the columns you need?**
A: It future-proofs the raw layer — if a modeling need surfaces later in dbt that requires a field I didn't originally extract into its own column, it's still there in `raw_payload` rather than permanently lost. Typed columns give fast, indexable access to the fields I know I need now; the JSONB column is the safety net.
