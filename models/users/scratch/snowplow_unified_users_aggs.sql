{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "start_tstamp",
      "data_type": "timestamp"
    }),
    cluster_by=snowplow_utils.get_value_by_target_type(bigquery_val=["domain_userid"]),
    sort='domain_userid',
    dist='domain_userid',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
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

from {{ ref('snowplow_unified_users_sessions_this_run') }}

group by 1,2,3
