with ci as (

    select * from {{ ref('stg_cmdb_ci') }}

),

users as (

    select * from {{ ref('stg_sys_user') }}

),

groups as (

    select * from {{ ref('stg_sys_user_group') }}

),

final as (

    select
        c.ci_id,
        c.ci_name,
        c.ci_class,
        c.operational_status,
        c.install_status,
        c.category,
        c.subcategory,
        c.assigned_to_id,
        u.full_name             as assigned_to_name,
        c.support_group_id,
        g.group_name            as support_group_name,
        c.created_at,
        c.updated_at

    from ci c
    left join users u on c.assigned_to_id = u.user_id
    left join groups g on c.support_group_id = g.assignment_group_id

)

select * from final
