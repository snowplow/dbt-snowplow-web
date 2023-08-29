{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    tags=["this_run"],
    enabled=var("snowplow__enable_cwv", false) and target.type == 'bigquery' | as_bool(),
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
    e.dvce_created_tstamp,
    e.load_tstamp,
    e.geo_country,
    e.page_url,
    e.page_title,
    e.useragent,

    {{ snowplow_utils.get_optional_fields(
        enabled=true,
        fields=yauaa_fields(),
        col_prefix='contexts_nl_basjes_yauaa_context_1_0_0',
        relation=ref('snowplow_unified_base_events_this_run'),
        relation_alias='e') }},

    {{ snowplow_utils.get_optional_fields(
        enabled= true,
        fields=[{'field': 'lcp', 'dtype': 'string'}, {'field': 'fcp', 'dtype': 'string'}, {'field': 'fid', 'dtype': 'string'}, {'field': 'cls', 'dtype': 'string'}, {'field': 'inp', 'dtype': 'string'}, {'field': 'ttfb', 'dtype': 'string'}, {'field': 'navigation_type', 'dtype': 'string'}],
        col_prefix='unstruct_event_com_snowplowanalytics_snowplow_unified_web_vitals_1_0_0',
        relation=ref('snowplow_unified_base_events_this_run'),
        relation_alias='e') }}

  from {{ ref("snowplow_unified_base_events_this_run") }} as e

  where {{ snowplow_utils.is_run_with_new_events('snowplow_unified') }} --returns false if run doesn't contain new events.

  and event_name = 'web_vitals'

  and page_view_id is not null

  -- exclude bot traffic

  {% if var('snowplow__enable_iab', false) %}
    and not {{ snowplow_utils.get_field(column_name = 'contexts_com_iab_snowplow_spiders_and_robots_1_0_0',
                              field_name = 'spider_or_robot',
                              table_alias = 'e',
                              type = 'boolean',
                              array_index = 0)}} = True
  {% endif %}

  {{ filter_bots() }}

)

select
  event_id,
  event_name,
  app_id,
  platform,
  domain_userid,
  original_domain_userid,
  user_id,
  page_view_id,
  domain_sessionid,
  original_domain_sessionid,
  collector_tstamp,
  derived_tstamp,
  dvce_created_tstamp,
  load_tstamp,
  geo_country,
  page_url,
  page_title,
  useragent,
  lower(device_class) as device_class,
  agent_class,
  agent_name,
  agent_name_version,
  agent_name_version_major,
  agent_version,
  agent_version_major,
  device_brand,
  device_name,
  device_version,
  layout_engine_class,
  layout_engine_name,
  layout_engine_name_version,
  layout_engine_name_version_major,
  layout_engine_version,
  layout_engine_version_major,
  operating_system_class,
  operating_system_name,
  operating_system_name_version,
  operating_system_version,
  ceil(cast(lcp as decimal)) /1000 as lcp,
  ceil(cast(fcp as decimal)) /1000 as fcp,
  ceil(safe_cast(fid as decimal) * 1000) /1000 as fid,
  ceil(cast(cls as decimal) * 1000) /1000 as cls,
  ceil(cast(inp as decimal) * 1000) /1000 as inp,
  ceil(cast(ttfb as decimal) * 1000) /1000 as ttfb,
  navigation_type

from prep p
