{{ 
  config(
    materialized='table',
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=["domain_userid"],
    sort='start_tstamp',
    dist='domain_userid',
    tags=["this_run"]
  ) 
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(model=ref('snowplow_web_users_userids_this_run'),
                                                                           lower_limit_col='start_tstamp',
                                                                           upper_limit_col='start_tstamp') %}


select
  a.*,
  min(a.start_tstamp) over(partition by a.domain_userid) as user_start_tstamp,
  max(a.end_tstamp) over(partition by a.domain_userid) as user_end_tstamp 

from {{ var('snowplow__sessions_table') }} a
inner join {{ ref('snowplow_web_users_userids_this_run') }} b
on a.domain_userid = b.domain_userid

where a.start_tstamp >= {{ lower_limit }}
and   a.start_tstamp <= {{ upper_limit }}
