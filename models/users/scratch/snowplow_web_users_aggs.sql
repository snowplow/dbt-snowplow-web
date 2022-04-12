{{ 
  config(
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid"]),
    sort='domain_userid',
    dist='domain_userid'
  ) 
}}

select
  domain_userid,
  -- time
  user_start_tstamp as start_tstamp,
  user_end_tstamp as end_tstamp,
  -- first/last session. Max to resolve edge case with multiple sessions with the same start/end tstamp
  max(case when start_tstamp = user_start_tstamp then domain_sessionid end) as first_domain_sessionid,
  max(case when end_tstamp = user_end_tstamp then domain_sessionid end) as last_domain_sessionid,
  -- engagement
  sum(page_views) as page_views,
  count(distinct domain_sessionid) as sessions,
  sum(engaged_time_in_s) as engaged_time_in_s

from {{ ref('snowplow_web_users_sessions_this_run') }}

group by 1,2,3
