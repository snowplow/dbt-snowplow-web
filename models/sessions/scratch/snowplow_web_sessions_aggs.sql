{{ 
  config(
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["domain_sessionid"]),
    sort='domain_sessionid',
    dist='domain_sessionid'
  ) 
}}

select
  domain_sessionid,
  -- time
  min(start_tstamp) as start_tstamp,
  max(end_tstamp) as end_tstamp,

  -- engagement
  count(distinct page_view_id) as page_views,
  sum(engaged_time_in_s) as engaged_time_in_s

from {{ ref('snowplow_web_page_views_this_run') }}

where domain_sessionid is not null
group by 1
