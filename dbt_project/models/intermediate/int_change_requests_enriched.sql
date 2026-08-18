with change_requests as (

    select * from {{ ref('stg_change_requests') }}

),

assignment_groups as (

    select * from {{ ref('stg_sys_user_group') }}

),

users as (

    select * from {{ ref('stg_sys_user') }}

),

cmdb as (

    select * from {{ ref('stg_cmdb_ci') }}

),

-- Illustrative planned-window thresholds by priority (demo data has no
-- documented change policy, so these are reasonable defaults, not
-- sourced from a real policy document). Priority 1 = most urgent =
-- shortest allowed execution window.
window_thresholds as (

    select * from (
        values
            ('1', 4),
            ('2', 8),
            ('3', 24),
            ('4', 48),
            ('5', 72)
    ) as t(priority, threshold_hours)

),

joined as (

    select
        c.change_request_id,
        c.change_number,
        c.short_description,
        c.change_type,
        c.risk,
        c.priority,
        c.state,
        c.approval,
        c.assignment_group_id,
        ag.group_name       as assignment_group_name,
        c.assigned_to_id,
        u.full_name         as assigned_to_name,
        c.ci_id,
        cmdb.ci_name         as cmdb_name,
        c.start_date,
        c.end_date,
        c.created_at,
        c.updated_at,
        wt.threshold_hours,

        -- completed changes: measure actual execution duration
        -- still-in-progress changes: measure time elapsed so far (a "currently over window" check)
        case
            when c.end_date is not null
                then extract(epoch from (c.end_date - c.start_date)) / 3600.0
            else extract(epoch from (now() - c.start_date)) / 3600.0
        end as change_window_hours,

        (c.end_date is null) as is_currently_in_progress,
        (c.start_date is not null) as has_valid_window

    from change_requests c
    left join assignment_groups ag on c.assignment_group_id = ag.assignment_group_id
    left join users u on c.assigned_to_id = u.user_id
    left join cmdb on c.ci_id = cmdb.ci_id
    left join window_thresholds wt on c.priority = wt.priority

),

final as (

    select
        *,
        (change_window_hours > threshold_hours) as exceeded_planned_window
    from joined

)

select * from final