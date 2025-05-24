{{ config(
    materialized='ephemeral'
) }}


select
    _dlt_parent_id,
    max(if(key = 'engagement_time_msec', value__int_value, null)) as engagement_time_msec,
    max(if(key = 'page_title', value__string_value, null)) as page_title,
    max(if(key = 'page_location', value__string_value, null)) as page_location,
    max(if(key = 'engaged_session_event', value__int_value, null)) as engaged_session_event,
    max(if(key = 'session_engaged', value__int_value, null)) as session_engaged,
    max(if(key = 'ga_session_id', value__int_value, null)) as ga_session_id,
    max(if(key = 'ga_session_number', value__int_value, null)) as ga_session_number
from {{ source('ga4_full_sample', 'ga4_events__event_params') }}
group by _dlt_parent_id