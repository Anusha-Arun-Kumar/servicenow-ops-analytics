-- This test fails if it returns any rows.
-- Business rule: a change request cannot end before it started.
-- Only meaningful for changes that actually have a start_date
-- (has_valid_window = true) — see docs/phase2-recap.md for why
-- ~29% of change requests have no scheduling data at all.

select
    change_request_id,
    change_number,
    start_date,
    end_date
from {{ ref('fct_change_requests') }}
where end_date is not null
  and start_date is not null
  and end_date < start_date
