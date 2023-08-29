{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}


with prep as (
  select
    domain_sessionid,
    count(distinct page_views_in_session) as dist_pvis_values,
    count(*) - count(distinct page_view_in_session_index)  as all_minus_dist_pvisi,
    count(*) - count(distinct page_view_id) as all_minus_dist_pvids

  from {{ ref('snowplow_unified_views') }}
  group by 1
)

select
  domain_sessionid

from prep

where dist_pvis_values != 1
or all_minus_dist_pvisi != 0
or all_minus_dist_pvids != 0
