-- This test fails if it returns any rows.
-- Business rule: an incident cannot be closed before it was opened.
-- Catches data quality issues in source timestamps that generic
-- not_null/unique tests can't express.

select
    incident_id,
    incident_number,
    opened_at,
    closed_at
from {{ ref('fct_incidents') }}
where closed_at is not null
  and closed_at < opened_at
