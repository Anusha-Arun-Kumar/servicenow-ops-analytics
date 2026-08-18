with source as (

    select * from {{ source('raw', 'cmdb_ci') }}

),

renamed as (

    select
        sys_id              as ci_id,
        name                as ci_name,
        sys_class_name      as ci_class,
        operational_status,
        install_status,
        category,
        subcategory,
        assigned_to         as assigned_to_id,
        support_group       as support_group_id,
        sys_created_on      as created_at,
        sys_updated_on      as updated_at

    from source

)

select * from renamed
