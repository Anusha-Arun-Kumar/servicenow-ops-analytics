with source as (

    select * from {{ source('raw', 'incidents') }}

),

renamed as (

    select
        sys_id              as incident_id,
        number              as incident_number,
        short_description,
        priority,
        state,
        assignment_group    as assignment_group_id,
        assigned_to         as assigned_to_id,
        opened_at,
        closed_at,
        sys_created_on      as created_at,
        sys_updated_on      as updated_at

    from source

)

select * from renamed
