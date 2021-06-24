{{ 
  config(
    materialized='table',
    sort='domain_userid',
    dist='domain_userid'
  ) 
}}

select
  domain_userid,
  min(start_tstamp) as min_tstamp

from {{ ref('snowplow_web_sessions_this_run') }}
group by 1
