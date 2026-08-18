with incidents as (

    select * from {{ ref('int_incidents_enriched') }}

),

final as (

    select
        incident_id,
        incident_number,
        short_description,
        priority,
        state,
        assignment_group_id,
        assignment_group_name,
        assigned_to_id,
        assigned_to_name,
        opened_at,
        closed_at,
        created_at,
        updated_at,
        sla_hours,
        resolution_time_hours,
        is_currently_open,
        sla_breached

    from incidents

)

select * from final
