# Phase 0 Recap: Environment Setup

## What Phase 0 accomplished
Set up the full local development environment: a ServiceNow data source, a GitHub repo, Python tooling, and local infrastructure (Postgres + Airflow) — everything needed before writing real pipeline logic in Phase 1.

---

## Step 1: ServiceNow Personal Developer Instance
- Signed up at developer.servicenow.com and provisioned a free personal dev instance
- Comes pre-loaded with demo ITSM data: incidents, changes, problems, CMDB records
- This is the **data source** — everything downstream extracts from here via the Table REST API
- Instance is fully sandboxed, unrelated to any real company data

## Step 2: GitHub repo + local Python tooling
- Created `servicenow-ops-analytics` repo on GitHub.com (with Python `.gitignore` template)
- Cloned it locally with `git clone`
- Created a Python virtual environment (`venv`) to isolate this project's packages from the rest of your system
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```
- Installed initial dependencies: `requests` (API calls), `python-dotenv` (secrets loading), `tenacity` (retry logic — to be used in Phase 1)
- Created folder skeleton: `dags/`, `dbt_project/`, `extractors/`, `.github/workflows/`

**Key concept learned — `.env` files:**
- `.env` holds secrets (API credentials) as plain `KEY=value` pairs, never committed to git
- `python-dotenv`'s `load_dotenv()` reads that file and injects values into `os.environ` at runtime
- `.env.example` (committed, no real values) documents what variables are needed for anyone setting up the project
- `.gitignore` must include `.env` — this is what actually prevents secrets from being pushed to GitHub

**Key concept learned — GitHub auth:**
- GitHub no longer accepts plain passwords for `git push`
- Used a **Personal Access Token (PAT)** generated from GitHub Settings → Developer Settings, used in place of a password when prompted
- Also cleaned up an accidentally-committed `.DS_Store` (macOS Finder metadata file) using `git rm --cached` and added it to `.gitignore`

**Wrote and tested `extractors/extract_incidents.py`:**
- First working script: loads credentials via `load_dotenv()`, calls ServiceNow's `/api/now/table/incident` endpoint with basic auth, prints the results
- This was a pure connectivity test — no pagination, incremental logic, or database writes yet (that's Phase 1)
- Debugged an issue where `SERVICENOW_INSTANCE` accidentally contained a full login-form URL with embedded credentials instead of just the clean base URL

## Step 3: Local infrastructure — Docker Compose (Postgres + Airflow)
- Installed Docker Desktop
- Wrote a `docker-compose.yml` defining four services:
  - **postgres** — a Postgres 15 container, used as both Airflow's internal metadata store and (via a separate database) your actual data warehouse
  - **airflow-init** — a one-time setup container that initializes Airflow's internal database and creates the admin user
  - **airflow-webserver** — serves the Airflow UI at `localhost:8080`
  - **airflow-scheduler** — the process that actually triggers and runs DAGs on schedule
- Started everything with `docker compose up -d`
- Verified all containers were running with `docker compose ps`

**Key concept learned — why Airflow needs LocalExecutor:**
- `LocalExecutor` runs tasks as local processes on the same machine — the simplest executor, appropriate for local dev/learning (production setups often use `CeleryExecutor` or `KubernetesExecutor` to distribute tasks across multiple machines)

**Security follow-up:**
- Default Airflow admin credentials (`admin`/`admin`) triggered a browser data-breach warning (expected — it's an extremely common credential pair, flagged regardless of whether *this specific* instance was compromised)
- Deleted and recreated the Airflow user with a stronger password, then updated `docker-compose.yml` so the password persists correctly if containers are ever rebuilt from scratch

## Step 4: Verified Airflow UI
- Confirmed `localhost:8080` loads the Airflow web UI and logs in successfully with the admin account
- DAGs list is empty — expected, since `dags/` folder has no DAG files yet (that comes in Phase 3)

## Step 5: Connected via DBeaver + created the warehouse database
- Installed DBeaver Community Edition
- Created a connection to the Postgres container (`localhost:5432`, user `airflow`)
- Ran `CREATE DATABASE servicenow_dw;` to create a **separate database** from Airflow's internal `airflow` database — keeps your actual pipeline data cleanly apart from Airflow's own metadata tables
- Created a second DBeaver connection pointed directly at `servicenow_dw` for ongoing use
- Verified the empty `public` schema exists, ready to receive tables in Phase 1

---

## Architecture at the end of Phase 0

```
ServiceNow Dev Instance (API source)
        |
        | (Python + requests, not yet built)
        v
extractors/extract_incidents.py  ---writes to--->  Postgres: servicenow_dw (empty, ready)
                                                            ^
                                                            |
                                              Airflow (localhost:8080) will
                                              orchestrate this in Phase 3
                                                            |
Postgres: airflow (Airflow's own internal metadata — separate from servicenow_dw)
```

## Tools now installed/configured
| Tool | Purpose | Access |
|---|---|---|
| ServiceNow Dev Instance | Source ITSM data | `https://dev268216.service-now.com` |
| GitHub repo | Version control | `github.com/Anusha-Arun-Kumar/servicenow-ops-analytics` |
| Python venv | Isolated dependencies | `source venv/bin/activate` |
| Docker Desktop | Runs Postgres + Airflow containers | — |
| Postgres (`airflow` db) | Airflow's internal metadata | via DBeaver, port 5432 |
| Postgres (`servicenow_dw` db) | Your actual data warehouse | via DBeaver, port 5432 |
| Airflow UI | Orchestration dashboard | `localhost:8080` |
| DBeaver | Database client/GUI | local app |

## New concepts you now have hands-on experience with
- API authentication + environment variable management for secrets
- Git/GitHub token-based authentication and `.gitignore` hygiene
- Docker Compose: multi-container orchestration, service dependencies, healthchecks
- Airflow's core components: webserver, scheduler, executor, metadata DB
- Separating "infrastructure" databases from "application" databases in the same Postgres instance
