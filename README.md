# ServiceNow Ops Analytics Pipeline

An end-to-end data engineering project: extracting ITSM data from a live ServiceNow instance, modeling it into a tested dimensional warehouse with dbt, orchestrating the full pipeline with Airflow, and visualizing it in dashboards built for different operational stakeholders.

Built as a hands-on portfolio project to demonstrate production-style data engineering practices — incremental extraction, retry/failure handling, dimensional modeling, automated testing, workflow orchestration, and dashboarding — end to end, on real (if small-scale) ITSM data.

---

## Architecture

```
ServiceNow (Personal Developer Instance)
        |
        | Table REST API — paginated, incremental, retried
        v
Python extractors (shared common.py module)
        |
        v
raw schema (Postgres)  ── 5 tables, upserted, current-state
        |
        | dbt: staging -> intermediate -> marts
        v
analytics schema (Postgres) ── 3 dimensions, 2 facts, 28 passing tests
        |
        v
Metabase dashboards ── ops leadership / change management views

Airflow orchestrates extraction + dbt run + dbt test on a daily schedule,
running fully containerized via Docker Compose.
```

---

## Tech stack

| Layer | Tools |
|---|---|
| Source | ServiceNow Table REST API |
| Extraction | Python (`requests`, `tenacity`, `psycopg2`, `python-dotenv`) |
| Warehouse | PostgreSQL (local Docker + Neon cloud instance) |
| Transformation | dbt-core (Postgres adapter) |
| Orchestration | Apache Airflow (Dockerized, custom image) |
| Dashboarding | Metabase |
| Version control | Git / GitHub |

---

## What this project demonstrates

- **Incremental extraction** with watermark tracking, a bounded lookback window for first-run behavior, and a manual `--full-backfill` escape hatch
- **Resilient API handling** — pagination that survives ACL-filtered short pages, retry logic that distinguishes transient failures (retried with exponential backoff) from permanent ones (fail fast), and graceful handling of edge cases like a hibernating ServiceNow instance
- **A layered dbt project** — staging (thin, 1:1) → intermediate (joins + business logic) → marts (dashboard-ready star schema: 2 fact tables, 3 conformed dimensions)
- **28 passing dbt tests** — source integrity, primary keys, referential integrity (scoped to allow legitimate nulls), accepted values, and custom business-rule tests
- **Explicit data quality handling** — e.g. a `has_valid_window` flag to honestly distinguish "no data" from "data that didn't breach," rather than silently coercing NULLs
- **A fully orchestrated, scheduled pipeline** — 5 parallel extraction tasks fanning into `dbt run` → `dbt test`, running inside a custom Docker image, with tested failure alerting
- **Dashboards built for distinct stakeholders** — not a single generic report, but views tailored to what each audience actually needs to act on

---

## Project structure

```
servicenow-ops-analytics/
├── extractors/
│   ├── common.py              # shared pagination, retry, watermark, upsert-flow logic
│   ├── extract_incidents.py
│   ├── extract_change_requests.py
│   ├── extract_sys_user_group.py
│   ├── extract_sys_user.py
│   └── extract_cmdb_ci.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/            # 5 thin, 1:1 source models
│   │   ├── intermediate/       # joins + SLA/window business logic
│   │   └── marts/              # 3 dimensions, 2 facts, schema tests
│   └── tests/                  # custom singular business-rule tests
├── dags/
│   └── servicenow_pipeline.py  # Airflow DAG: extract (parallel) -> dbt run -> dbt test
├── sql/
│   └── raw_schema.sql          # reproducible raw-layer DDL
├── docs/                       # phase-by-phase build notes, good-to-knows, interview Q&A
├── docker-compose.yml          # Postgres, Airflow (custom image), Metabase
├── Dockerfile                  # custom Airflow image with project dependencies
├── requirements.txt
└── .env.example
```

---

## Data model

**Sources (raw, upserted on `sys_id`):** `incidents`, `change_requests`, `sys_user_group`, `sys_user`, `cmdb_ci`

**Marts:**
- `fct_incidents` — one row per incident, with resolved assignment group/user names and a priority-tiered SLA breach calculation
- `fct_change_requests` — one row per change, with resolved names and planned-window duration tracking (~29% of records have no scheduling data, explicitly flagged via `has_valid_window` rather than silently coerced)
- `dim_assignment_group`, `dim_user`, `dim_ci` — fully resolved, "leaf" dimensions (including self-joins for manager/parent-group hierarchies)

---

## Running this locally

1. Provision a free [ServiceNow Personal Developer Instance](https://developer.servicenow.com)
2. Copy `.env.example` to `.env` and fill in your ServiceNow and Postgres credentials
3. `docker compose up -d` — brings up Postgres, Airflow, and Metabase
4. Run `sql/raw_schema.sql` against your warehouse to create the `raw` schema
5. Run each extractor once with `--full-backfill` to seed initial data
6. `cd dbt_project && dbt run && dbt test` to build and validate the models
7. Trigger the `servicenow_pipeline` DAG in the Airflow UI (`localhost:8080`) to verify end-to-end orchestration
8. Connect Metabase (`localhost:3000`) to the warehouse and explore the `analytics` schema

---

## Documentation

Phase-by-phase build notes — including real bugs encountered and fixed, and design decisions and their tradeoffs — live in [`docs/`](docs/).

---

## Author

Built by Anusha Arun Kumar as a hands-on portfolio project to build production-style data engineering experience alongside day-to-day work on the ServiceNow platform.
