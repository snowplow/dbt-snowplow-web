{{ 
  config(
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=["domain_userid"],
    sort='domain_userid',
    dist='domain_userid'
  ) 
}}

select
  domain_userid,
  -- time
  min(start_tstamp) as start_tstamp,
  max(end_tstamp) as end_tstamp,

  -- engagement
  sum(page_views) as page_views,
  count(distinct domain_sessionid) as sessions,
  sum(engaged_time_in_s) as engaged_time_in_s

from {{ ref('snowplow_web_users_sessions_this_run') }}

group by 1
