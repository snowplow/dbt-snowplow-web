{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    tags=["this_run"]
  )
}}

with prep as (
select
  ev.page_view_id,
  ev.event_id,

  ev.app_id,
  ev.platform,

  -- user fields
  ev.user_id,
  ev.domain_userid,
  ev.original_domain_userid,
  {% if var('snowplow__page_view_stitching') %}
    -- updated with mapping as part of post hook on derived page_views table
    cast(domain_userid as {{ type_string() }}) as stitched_user_id,
  {% else %}
    cast(null as {{ type_string() }}) as stitched_user_id,
  {% endif %}
  ev.network_userid,

  -- session fields
  ev.domain_sessionid,
  ev.original_domain_sessionid,
  ev.domain_sessionidx,

  -- timestamp fields
  ev.dvce_created_tstamp,
  ev.collector_tstamp,
  ev.derived_tstamp,
  ev.derived_tstamp as start_tstamp,

  ev.doc_width,
  ev.doc_height,

  ev.page_title,
  {{ content_group_query() }} as content_group,
  ev.page_url,
  ev.page_urlscheme,
  ev.page_urlhost,
  ev.page_urlpath,
  ev.page_urlquery,
  ev.page_urlfragment,

  -- marketing fields
  ev.mkt_medium,
  ev.mkt_source,
  ev.mkt_term,
  ev.mkt_content,
  ev.mkt_campaign,
  ev.mkt_clickid,
  ev.mkt_network,
  {{ channel_group_query() }} as default_channel_group,

  -- referrer fields
  ev.page_referrer,
  ev.refr_urlscheme,
  ev.refr_urlhost,
  ev.refr_urlpath,
  ev.refr_urlquery,
  ev.refr_urlfragment,
  ev.refr_medium,
  ev.refr_source,
  ev.refr_term,

  -- geo fields
  ev.geo_country,
  ev.geo_region,
  ev.geo_region_name,
  ev.geo_city,
  ev.geo_zipcode,
  ev.geo_latitude,
  ev.geo_longitude,
  ev.geo_timezone ,

  ev.user_ipaddress,

  ev.useragent,

  ev.dvce_screenwidth || 'x' || ev.dvce_screenheight as screen_resolution,

  ev.br_lang,
  ev.br_viewwidth,
  ev.br_viewheight,
  ev.br_colordepth,
  ev.br_renderengine,
  ev.os_timezone,


  -- optional fields, only populated if enabled.

  -- iab enrichment fields: set iab variable to true to enable
  {{ snowplow_utils.get_optional_fields(
        enabled=var('snowplow__enable_iab', false),
        fields=iab_fields(),
        col_prefix='contexts_com_iab_snowplow_spiders_and_robots_1',
        relation=ref('snowplow_unified_base_events_this_run'),
        relation_alias='ev') }},

  -- ua parser enrichment fields: set ua_parser variable to true to enable
  {{ snowplow_utils.get_optional_fields(
        enabled=var('snowplow__enable_ua', false),
        fields=ua_fields(),
        col_prefix='contexts_com_snowplowanalytics_snowplow_ua_parser_context_1',
        relation=ref('snowplow_unified_base_events_this_run'),
        relation_alias='ev') }},

  -- yauaa enrichment fields: set yauaa variable to true to enable
  {{ snowplow_utils.get_optional_fields(
        enabled=var('snowplow__enable_yauaa', false),
        fields=yauaa_fields(),
        col_prefix='contexts_nl_basjes_yauaa_context_1',
        relation=ref('snowplow_unified_base_events_this_run'),
        relation_alias='ev') }}

  {%- if var('snowplow__page_view_passthroughs', []) -%}
    {%- set passthrough_names = [] -%}
    {%- for identifier in var('snowplow__page_view_passthroughs', []) %}
    {# Check if it's a simple column or a sql+alias #}
      {%- if identifier is mapping -%}
        ,{{identifier['sql']}} as {{identifier['alias']}}
        {%- do passthrough_names.append(identifier['alias']) -%}
      {%- else -%}
        ,ev.{{identifier}}
        {%- do passthrough_names.append(identifier) -%}
      {%- endif -%}
    {% endfor -%}
  {%- endif %}

  from {{ ref('snowplow_unified_base_events_this_run') }} as ev
  left join {{ ref(var('snowplow__ga4_categories_seed')) }} c on lower(trim(ev.mkt_source)) = lower(c.source)

  where ev.event_name = 'page_view'
  and ev.page_view_id is not null

  {% if var("snowplow__ua_bot_filter", true) %}
  {{ filter_bots('ev') }}
  {% endif %}

  qualify row_number() over (partition by ev.page_view_id order by ev.derived_tstamp, ev.dvce_created_tstamp) = 1
)

, page_view_events as (

  select
    ev.page_view_id,
    ev.event_id,

    ev.app_id,
    ev.platform,

    -- user fields
    ev.user_id,
    ev.domain_userid,
    ev.original_domain_userid,
    ev.stitched_user_id,
    ev.network_userid,

    -- session fields
    ev.domain_sessionid,
    ev.original_domain_sessionid,
    ev.domain_sessionidx,

    row_number() over (partition by ev.domain_sessionid order by ev.derived_tstamp, ev.dvce_created_tstamp, ev.event_id) AS page_view_in_session_index,

    -- timestamp fields
    ev.dvce_created_tstamp,
    ev.collector_tstamp,
    ev.derived_tstamp,
    ev.start_tstamp,
    coalesce(t.end_tstamp, ev.derived_tstamp) as end_tstamp, -- only page views with pings will have a row in table t
    {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp,

    coalesce(t.engaged_time_in_s, 0) as engaged_time_in_s, -- where there are no pings, engaged time is 0.
    {{ datediff('ev.derived_tstamp', 'coalesce(t.end_tstamp, ev.derived_tstamp)', 'second') }} as absolute_time_in_s,

    sd.hmax as horizontal_pixels_scrolled,
    sd.vmax as vertical_pixels_scrolled,

    sd.relative_hmax as horizontal_percentage_scrolled,
    sd.relative_vmax as vertical_percentage_scrolled,

    ev.doc_width,
    ev.doc_height,
    ev.content_group,

    ev.page_title,
    ev.page_url,
    ev.page_urlscheme,
    ev.page_urlhost,
    ev.page_urlpath,
    ev.page_urlquery,
    ev.page_urlfragment,

    ev.mkt_medium,
    ev.mkt_source,
    ev.mkt_term,
    ev.mkt_content,
    ev.mkt_campaign,
    ev.mkt_clickid,
    ev.mkt_network,
    ev.default_channel_group,

    ev.page_referrer,
    ev.refr_urlscheme,
    ev.refr_urlhost,
    ev.refr_urlpath,
    ev.refr_urlquery,
    ev.refr_urlfragment,
    ev.refr_medium,
    ev.refr_source,
    ev.refr_term,

    ev.geo_country,
    ev.geo_region,
    ev.geo_region_name,
    ev.geo_city,
    ev.geo_zipcode,
    ev.geo_latitude,
    ev.geo_longitude,
    ev.geo_timezone,

    ev.user_ipaddress,

    ev.useragent,

    ev.br_lang,
    ev.br_viewwidth,
    ev.br_viewheight,
    ev.br_colordepth,
    ev.br_renderengine,

    ev.os_timezone,

    ev.category,
    ev.primary_impact,
    ev.reason,
    ev.spider_or_robot,

    ev.useragent_family,
    ev.useragent_major,
    ev.useragent_minor,
    ev.useragent_patch,
    ev.useragent_version,
    ev.os_family,
    ev.os_major,
    ev.os_minor,
    ev.os_patch,
    ev.os_patch_minor,
    ev.os_version,
    ev.device_family,

    ev.device_class,
    case when ev.device_class = 'Desktop' then 'Desktop'
      when ev.device_class = 'Phone' then 'Mobile'
      when ev.device_class = 'Tablet' then 'Tablet'
      else 'Other' end as device_category,
    ev.screen_resolution,
    ev.agent_class,
    ev.agent_name,
    ev.agent_name_version,
    ev.agent_name_version_major,
    ev.agent_version,
    ev.agent_version_major,
    ev.device_brand,
    ev.device_name,
    ev.device_version,
    ev.layout_engine_class,
    ev.layout_engine_name,
    ev.layout_engine_name_version,
    ev.layout_engine_name_version_major,
    ev.layout_engine_version,
    ev.layout_engine_version_major,
    ev.operating_system_class,
    ev.operating_system_name,
    ev.operating_system_name_version,
    ev.operating_system_version
    {%- if var('snowplow__page_view_passthroughs', []) -%}
      {%- for col in passthrough_names %}
        , ev.{{col}}
      {%- endfor -%}
    {%- endif %}

  from prep ev

  left join {{ ref('snowplow_unified_pv_engaged_time') }} t
  on ev.page_view_id = t.page_view_id {% if var('snowplow__limit_page_views_to_session', true) %} and ev.domain_sessionid = t.domain_sessionid {% endif %}

  left join {{ ref('snowplow_unified_pv_scroll_depth') }} sd
  on ev.page_view_id = sd.page_view_id {% if var('snowplow__limit_page_views_to_session', true) %} and ev.domain_sessionid = sd.domain_sessionid {% endif %}

)

select
   ev.page_view_id,
    ev.event_id,

    ev.app_id,
    ev.platform,

    -- user fields
    ev.user_id,
    ev.domain_userid,
    ev.original_domain_userid,
    ev.stitched_user_id,
    ev.network_userid,

    -- session fields
    ev.domain_sessionid,
    ev.original_domain_sessionid,
    ev.domain_sessionidx,

    ev.page_view_in_session_index,
    max(ev.page_view_in_session_index) over (partition by ev.domain_sessionid) as page_views_in_session,

    -- timestamp fields
    ev.dvce_created_tstamp,
    ev.collector_tstamp,
    ev.derived_tstamp,
    ev.start_tstamp,
    ev.end_tstamp,
    ev.model_tstamp,

    ev.engaged_time_in_s,
    ev.absolute_time_in_s,

    ev.horizontal_pixels_scrolled,
    ev.vertical_pixels_scrolled,

    ev.horizontal_percentage_scrolled,
    ev.vertical_percentage_scrolled,

    ev.doc_width,
    ev.doc_height,
    ev.content_group,

    ev.page_title,
    ev.page_url,
    ev.page_urlscheme,
    ev.page_urlhost,
    ev.page_urlpath,
    ev.page_urlquery,
    ev.page_urlfragment,

    ev.mkt_medium,
    ev.mkt_source,
    ev.mkt_term,
    ev.mkt_content,
    ev.mkt_campaign,
    ev.mkt_clickid,
    ev.mkt_network,
    ev.default_channel_group,

    ev.page_referrer,
    ev.refr_urlscheme,
    ev.refr_urlhost,
    ev.refr_urlpath,
    ev.refr_urlquery,
    ev.refr_urlfragment,
    ev.refr_medium,
    ev.refr_source,
    ev.refr_term,

    ev.geo_country,
    ev.geo_region,
    ev.geo_region_name,
    ev.geo_city,
    ev.geo_zipcode,
    ev.geo_latitude,
    ev.geo_longitude,
    ev.geo_timezone,

    ev.user_ipaddress,

    ev.useragent,

    ev.br_lang,
    ev.br_viewwidth,
    ev.br_viewheight,
    ev.br_colordepth,
    ev.br_renderengine,

    ev.os_timezone,

    ev.category,
    ev.primary_impact,
    ev.reason,
    ev.spider_or_robot,

    ev.useragent_family,
    ev.useragent_major,
    ev.useragent_minor,
    ev.useragent_patch,
    ev.useragent_version,
    ev.os_family,
    ev.os_major,
    ev.os_minor,
    ev.os_patch,
    ev.os_patch_minor,
    ev.os_version,
    ev.device_family,

    ev.device_class,
    ev.device_category,
    ev.screen_resolution,
    ev.agent_class,
    ev.agent_name,
    ev.agent_name_version,
    ev.agent_name_version_major,
    ev.agent_version,
    ev.agent_version_major,
    ev.device_brand,
    ev.device_name,
    ev.device_version,
    ev.layout_engine_class,
    ev.layout_engine_name,
    ev.layout_engine_name_version,
    ev.layout_engine_name_version_major,
    ev.layout_engine_version,
    ev.layout_engine_version_major,
    ev.operating_system_class,
    ev.operating_system_name,
    ev.operating_system_name_version,
    ev.operating_system_version
    {%- if var('snowplow__page_view_passthroughs', []) -%}
      {%- for col in passthrough_names %}
        , ev.{{col}}
      {%- endfor -%}
    {%- endif %}

from page_view_events ev
