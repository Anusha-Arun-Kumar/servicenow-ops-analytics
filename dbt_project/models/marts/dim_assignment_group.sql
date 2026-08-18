with groups as (

    select * from {{ ref('stg_sys_user_group') }}

),

users as (

    select * from {{ ref('stg_sys_user') }}

),

parent_groups as (

    -- self-referencing join: groups can be nested under other groups
    select * from {{ ref('stg_sys_user_group') }}

),

final as (

    select
        g.assignment_group_id,
        g.group_name,
        g.description,
        g.active,
        g.manager_id,
        m.full_name             as manager_name,
        g.parent_group_id,
        pg.group_name            as parent_group_name,
        g.email,
        g.created_at,
        g.updated_at

    from groups g
    left join users m on g.manager_id = m.user_id
    left join parent_groups pg on g.parent_group_id = pg.assignment_group_id

)

select * from final
