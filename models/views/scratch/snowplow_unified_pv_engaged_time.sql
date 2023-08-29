{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select
  ev.page_view_id,
  {% if var('snowplow__limit_page_views_to_session', true) %}
  ev.domain_sessionid,
  {% endif %}
  max(ev.derived_tstamp) as end_tstamp,

  -- aggregate pings:
    -- divides epoch tstamps by snowplow__heartbeat to get distinct intervals
    -- floor rounds to nearest integer - duplicates all evaluate to the same number
    -- count(distinct) counts duplicates only once
    -- adding snowplow__min_visit_length accounts for the page view event itself.

  {{ var("snowplow__heartbeat", 10) }} * (count(distinct(floor({{ snowplow_utils.to_unixtstamp('ev.dvce_created_tstamp') }}/{{ var("snowplow__heartbeat", 10) }}))) - 1) + {{ var("snowplow__min_visit_length", 5) }} as engaged_time_in_s

from {{ ref('snowplow_unified_base_events_this_run') }} as ev

where ev.event_name = 'page_ping'
and ev.page_view_id is not null

group by 1 {% if var('snowplow__limit_page_views_to_session', true) %}, 2 {% endif %}
