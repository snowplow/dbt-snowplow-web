{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

-- materialized as a view since we are just joining two production tables.
{{ 
  config(
    materialized='view',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  ) 
}}


select 
  pv.*,
  ce.link_clicks,
  ce.first_link_target,
  ce.is_bounced_page_view,
  ce.engagement_score,
  ce.channel

from {{ ref('snowplow_web_page_views') }} pv -- Join together the two incremental production tables
left join {{ ref('snowplow_web_pv_channel_engagement')}} ce
on pv.page_view_id = ce.page_view_id
