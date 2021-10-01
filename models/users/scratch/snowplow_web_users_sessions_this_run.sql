{{ 
  config(
    materialized='table',
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid"]),
    sort='start_tstamp',
    dist='domain_userid',
    tags=["this_run"]
  ) 
}}

with user_ids_this_run as (
select distinct domain_userid from {{ ref('snowplow_web_base_sessions_this_run') }}
)

select
  a.*,
  min(a.start_tstamp) over(partition by a.domain_userid) as user_start_tstamp,
  max(a.end_tstamp) over(partition by a.domain_userid) as user_end_tstamp 

from {{ var('snowplow__sessions_table') }} a
inner join user_ids_this_run b
on a.domain_userid = b.domain_userid
