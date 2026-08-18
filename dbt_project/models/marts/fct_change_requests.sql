with change_requests as (

    select * from {{ ref('int_change_requests_enriched') }}

),

final as (

    select
        change_request_id,
        change_number,
        short_description,
        change_type,
        risk,
        priority,
        state,
        approval,
        assignment_group_id,
        assignment_group_name,
        assigned_to_id,
        assigned_to_name,
        ci_id,
        cmdb_name,
        start_date,
        end_date,
        created_at,
        updated_at,
        threshold_hours,
        change_window_hours,
        is_currently_in_progress,
        has_valid_window,
        exceeded_planned_window

    from change_requests

)

select * from final
