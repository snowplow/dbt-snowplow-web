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


select * from {{ ref('snowplow_web_sessions_this_run') }}
