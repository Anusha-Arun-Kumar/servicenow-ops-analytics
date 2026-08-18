with source as (

    select * from {{ source('raw', 'change_requests') }}

),

renamed as (

    select
        sys_id              as change_request_id,
        number              as change_number,
        short_description,
        type                as change_type,
        risk,
        priority,
        state,
        approval,
        assignment_group    as assignment_group_id,
        assigned_to         as assigned_to_id,
        cmdb_ci             as ci_id,
        requested_by        as requested_by_id,
        start_date,
        end_date,
        sys_created_on      as created_at,
        sys_updated_on      as updated_at

    from source

)

select * from renamed
