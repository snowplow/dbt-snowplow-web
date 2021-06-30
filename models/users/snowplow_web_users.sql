{{ 
  config(
    enabled=var('snowplow__enable_derived_users'),
    materialized='snowplow_incremental',
    unique_key='domain_userid',
    upsert_date_key='start_tstamp',
    disable_upsert_lookback=true,
    sort='start_tstamp',
    dist='domain_userid'
  ) 
}}


select * 
from {{ ref('snowplow_web_users_this_run') }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
