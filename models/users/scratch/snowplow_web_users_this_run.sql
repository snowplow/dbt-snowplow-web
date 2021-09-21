{{ 
  config(
    materialized='table',
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["user_id"]),
    sort='start_tstamp',
    dist='domain_userid',
    tags=["this_run"]
  ) 
}}

select
  -- user fields
  a.user_id,
  a.domain_userid,
  a.network_userid,

  b.start_tstamp,
  b.end_tstamp,
  {{ dbt_utils.current_timestamp_in_utc() }} as model_tstamp,

  -- engagement fields
  b.page_views,
  b.sessions,

  b.engaged_time_in_s,

  -- first page fields
  a.first_page_title,

  a.first_page_url,

  a.first_page_urlscheme,
  a.first_page_urlhost,
  a.first_page_urlpath,
  a.first_page_urlquery,
  a.first_page_urlfragment,

  c.last_page_title,

  c.last_page_url,

  c.last_page_urlscheme,
  c.last_page_urlhost,
  c.last_page_urlpath,
  c.last_page_urlquery,
  c.last_page_urlfragment,

  -- referrer fields
  a.referrer,

  a.refr_urlscheme,
  a.refr_urlhost,
  a.refr_urlpath,
  a.refr_urlquery,
  a.refr_urlfragment,

  a.refr_medium,
  a.refr_source,
  a.refr_term,

  -- marketing fields
  a.mkt_medium,
  a.mkt_source,
  a.mkt_term,
  a.mkt_content,
  a.mkt_campaign,
  a.mkt_clickid,
  a.mkt_network

from {{ ref('snowplow_web_users_aggs') }} as b

inner join {{ ref('snowplow_web_users_sessions_this_run') }} as a
on a.domain_sessionid = b.first_domain_sessionid

inner join {{ ref('snowplow_web_users_lasts') }} c
on b.domain_userid = c.domain_userid
