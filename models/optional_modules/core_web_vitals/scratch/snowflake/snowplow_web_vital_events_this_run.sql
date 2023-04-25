{{
  config(
    tags=["this_run"],
    enabled=var("snowplow__enable_cwv", false) and target.type == 'snowflake' | as_bool(),
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
    e.user_id,
    e.page_view_id,
    e.domain_sessionid,
    e.collector_tstamp,
    e.derived_tstamp,
    e.dvce_created_tstamp,
    e.load_tstamp,
    e.geo_country,
    e.page_url,
    e.page_title,
    e.useragent,

    {{snowplow_web.get_yauaa_context_fields()}},

    ceil(e.unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1:lcp::decimal(9,4), 3) /1000 as lcp,
    ceil(e.unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1:fid::decimal(9,4), 3) as fid,
    ceil(e.unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1:cls::decimal(9,4), 3) as cls,
    ceil(e.unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1:inp::decimal(9,4), 3) as inp,
    ceil(e.unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1:ttfb::decimal(9,4), 3) as ttfb,
    e.unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1:navigationType::varchar as navigation_type

  from {{ ref("snowplow_web_base_events_this_run") }} as e

  where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.

  and event_name = 'web_vitals'

  and page_view_id is not null

  -- exclude bot traffic

  {% if var('snowplow__enable_iab', false) %}
    and not {{ snowplow_utils.get_field(column_name = 'contexts_com_iab_snowplow_spiders_and_robots_1',
                              field_name = 'spiderOrRobot',
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
  user_id,
  page_view_id,
  domain_sessionid,
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
  lcp,
  fid,
  cls,
  inp,
  ttfb,
  navigation_type

from prep p
