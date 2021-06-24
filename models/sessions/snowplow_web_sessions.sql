{{ 
  config(
    enabled=var('enable_derived_users'),
    materialized='snowplow_incremental',
    unique_key='domain_sessionid',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='domain_sessionid'
  ) 
}}


select * from {{ ref('snowplow_web_sessions_this_run') }}
