with source as (

    select * from {{ source('raw', 'sys_user_group') }}

),

renamed as (

    select
        sys_id              as assignment_group_id,
        name                as group_name,
        description,
        active,
        manager             as manager_id,
        parent              as parent_group_id,
        email,
        sys_created_on      as created_at,
        sys_updated_on      as updated_at

    from source

)

select * from renamed
