with incidents as (

    select * from {{ ref('stg_incidents') }}

),

assignment_groups as (

    select * from {{ ref('stg_sys_user_group') }}

),

users as (

    select * from {{ ref('stg_sys_user') }}

),

-- Illustrative SLA thresholds by priority (demo data has no documented
-- SLA policy, so these are reasonable defaults, not sourced from a real
-- policy document). Priority 1 = most urgent = shortest allowed window.
sla_thresholds as (

    select * from (
        values
            ('1', 4),
            ('2', 8),
            ('3', 24),
            ('4', 48),
            ('5', 72)
    ) as t(priority, sla_hours)

),

joined as (

    select
        i.incident_id,
        i.incident_number,
        i.short_description,
        i.priority,
        i.state,
        i.assignment_group_id,
        ag.group_name       as assignment_group_name,
        i.assigned_to_id,
        u.full_name         as assigned_to_name,
        i.opened_at,
        i.closed_at,
        i.created_at,
        i.updated_at,
        st.sla_hours,

        -- resolved incidents: measure actual resolution time
        -- still-open incidents: measure time elapsed so far (a "currently breaching" check)
        case
            when i.closed_at is not null
                then extract(epoch from (i.closed_at - i.opened_at)) / 3600.0
            else extract(epoch from (now() - i.opened_at)) / 3600.0
        end as resolution_time_hours,

        (i.closed_at is null) as is_currently_open

    from incidents i
    left join assignment_groups ag on i.assignment_group_id = ag.assignment_group_id
    left join users u on i.assigned_to_id = u.user_id
    left join sla_thresholds st on i.priority = st.priority

),

final as (

    select
        *,
        (resolution_time_hours > sla_hours) as sla_breached
    from joined

)

select * from final
