with users as (

    select * from {{ ref('stg_sys_user') }}

),

managers as (

    -- self-referencing join: a user's manager is also a sys_user record
    select * from {{ ref('stg_sys_user') }}

),

final as (

    select
        u.user_id,
        u.user_name,
        u.full_name,
        u.email,
        u.active,
        u.title,
        u.department_id,  -- not resolved to a name: no sys_user_department extractor built (documented scope limitation)
        u.manager_id,
        m.full_name          as manager_name,
        u.created_at,
        u.updated_at

    from users u
    left join managers m on u.manager_id = m.user_id

)

select * from final
