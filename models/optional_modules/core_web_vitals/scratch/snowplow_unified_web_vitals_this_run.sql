{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    enabled=var("snowplow__enable_cwv", false) | as_bool(),
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (

  select
    e.event_id,
    e.event_name,
    e.app_id,
    e.platform,
    e.domain_userid,
    e.original_domain_userid,
    e.user_id,
    e.page_view_id,
    e.domain_sessionid,
    e.original_domain_sessionid,
    e.collector_tstamp,
    e.derived_tstamp,
    e.load_tstamp,
    coalesce(e.geo_country, 'unknown_geo_country') as geo_country,
    coalesce(e.page_url, 'unknown_page_url') as page_url,
    {{ core_web_vital_page_groups() }} as url_group,
    e.page_title,
    e.useragent,
    coalesce(e.device_class, 'unknown_device_class') as device_class,
    e.device_name,
    e.agent_name,
    e.agent_version,
    e.operating_system_name,
    e.lcp,
    e.fcp,
    e.fid,
    e.cls,
    e.inp,
    e.ttfb,
    e.navigation_type,
    row_number() over (partition by e.page_view_id order by e.derived_tstamp, e.dvce_created_tstamp, e.event_id) dedupe_index

  from {{ ref("snowplow_unified_web_vital_events_this_run") }} as e

  where {{ snowplow_utils.is_run_with_new_events('snowplow_unified') }} --returns false if run doesn't contain new events.


)

select
  *,
  {{ snowplow_unified.core_web_vital_results_query() }}

from prep p

where dedupe_index = 1
