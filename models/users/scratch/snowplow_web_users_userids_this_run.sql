{{ 
  config(
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["domain_userid"],
    sort='domain_userid',
    dist='domain_userid',
    tags=["this_run"]
  ) 
}}

with user_ids_this_run as (
select distinct domain_userid from {{ ref('snowplow_web_base_sessions_this_run') }}
)

select
  a.domain_userid,
  min(a.start_tstamp) as start_tstamp

from {{ ref('snowplow_web_base_sessions_lifecycle_manifest') }}  a
inner join user_ids_this_run b
on a.domain_userid = b.domain_userid

group by 1
