-- ============================================================
-- Raw schema DDL — servicenow_dw
-- Run these against servicenow_dw to (re)create the raw layer
-- from scratch. Order matters only in that extraction_log has
-- no foreign keys, so it can go anywhere.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- Tracks every extraction run: watermark used/produced, record
-- counts, and success/failure status. Read by get_last_watermark()
-- before each run; written by start_extraction_log() / complete_extraction_log().
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

-- Fact table: incidents. One row per incident, always current state
-- (SCD Type 1 / upsert on sys_id). See docs/phase1-recap.md for the
-- SCD Type 1 vs Type 2 reasoning behind this design.
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

-- Fact table: change requests. Structurally similar to incidents —
-- a second ITSM process, same upsert pattern.
CREATE TABLE raw.change_requests (
    sys_id              VARCHAR(32) PRIMARY KEY,
    number              VARCHAR(20),
    short_description   TEXT,
    type                VARCHAR(20),
    risk                VARCHAR(20),
    priority            VARCHAR(10),
    state               VARCHAR(10),
    approval            VARCHAR(20),
    assignment_group    VARCHAR(32),
    assigned_to         VARCHAR(32),
    cmdb_ci             VARCHAR(32),
    requested_by        VARCHAR(32),
    start_date          TIMESTAMP,
    end_date            TIMESTAMP,
    sys_created_on      TIMESTAMP,
    sys_updated_on      TIMESTAMP NOT NULL,
    raw_payload         JSONB NOT NULL,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT now()
);

-- Dimension: assignment groups. Resolves incident/change_request's
-- assignment_group sys_id into a readable name (e.g. "Network", "Service Desk").
CREATE TABLE raw.sys_user_group (
    sys_id          VARCHAR(32) PRIMARY KEY,
    name            VARCHAR(255),
    description     TEXT,
    active          VARCHAR(10),
    manager         VARCHAR(32),
    parent          VARCHAR(32),
    email           VARCHAR(255),
    sys_created_on  TIMESTAMP,
    sys_updated_on  TIMESTAMP NOT NULL,
    raw_payload     JSONB NOT NULL,
    _loaded_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- Dimension: users. Resolves assigned_to / requested_by sys_ids into
-- readable names.
CREATE TABLE raw.sys_user (
    sys_id          VARCHAR(32) PRIMARY KEY,
    user_name       VARCHAR(255),
    name            VARCHAR(255),
    email           VARCHAR(255),
    active          VARCHAR(10),
    department      VARCHAR(32),
    manager         VARCHAR(32),
    title           VARCHAR(255),
    sys_created_on  TIMESTAMP,
    sys_updated_on  TIMESTAMP NOT NULL,
    raw_payload     JSONB NOT NULL,
    _loaded_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- Dimension: configuration items (CMDB). Resolves cmdb_ci references
-- on incidents/change_requests into readable asset names/types.
CREATE TABLE raw.cmdb_ci (
    sys_id              VARCHAR(32) PRIMARY KEY,
    name                VARCHAR(255),
    sys_class_name      VARCHAR(100),
    operational_status  VARCHAR(20),
    install_status      VARCHAR(20),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    assigned_to         VARCHAR(32),
    support_group       VARCHAR(32),
    sys_created_on      TIMESTAMP,
    sys_updated_on      TIMESTAMP NOT NULL,
    raw_payload         JSONB NOT NULL,
    _loaded_at          TIMESTAMP NOT NULL DEFAULT now()
);
