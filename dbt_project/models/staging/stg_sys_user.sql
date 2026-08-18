with source as (

    select * from {{ source('raw', 'sys_user') }}

),

renamed as (

    select
        sys_id              as user_id,
        user_name,
        name                as full_name,
        email,
        active,
        department          as department_id,
        manager             as manager_id,
        title,
        sys_created_on      as created_at,
        sys_updated_on      as updated_at

    from source

)

select * from renamed
