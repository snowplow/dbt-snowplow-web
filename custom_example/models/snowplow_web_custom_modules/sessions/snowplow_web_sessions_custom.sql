{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='domain_sessionid',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='domain_sessionid'
  ) 
}}


select 
  s.*,
  c.is_session_w_intent,
  c.is_session_w_conversion

from {{ ref('snowplow_web_sessions_this_run') }} s -- join sessions_this_run to sessions_conversion_this_run to produce complete sessions table
left join {{ ref('snowplow_web_sessions_conversion_this_run')}} c
on s.domain_sessionid = c.domain_sessionid

where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
