{{ 
  config(
    enabled=var('snowplow__enable_derived_sessions'),
    materialized='snowplow_incremental',
    unique_key='domain_sessionid',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='domain_sessionid'
  ) 
}}


select * 
from {{ ref('snowplow_web_sessions_this_run') }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
