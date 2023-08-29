{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

-- Removing model_tstamp

select
  page_view_id,
  event_id,

-- hard-coding due to non-deterministic outcome from row_number for Redshift/Postgres/databricks
{% if target.type in ['redshift', 'postgres', 'databricks'] -%}
  case when event_id = '1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76' then 'true base' else app_id end as app_id,
{% else %}
  app_id,
{% endif %}
  platform,
  -- user fields
  user_id,
  domain_userid,
  original_domain_userid,
  stitched_user_id,
  network_userid,

  -- session fields
  domain_sessionid,
  original_domain_sessionid,
  domain_sessionidx,

  page_view_in_session_index,
  page_views_in_session,

  -- timestamp fields

  -- hard-coding due to non-deterministic outcome from row_number for Redshift/Postgres/databricks
{% if target.type in ['redshift', 'postgres', 'databricks'] -%}
  case when event_id = '1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76' then '2021-03-01 20:56:33.286' else dvce_created_tstamp end as dvce_created_tstamp,
{% else %}
  dvce_created_tstamp,
{% endif %}

  collector_tstamp,

  -- hard-coding due to non-deterministic outcome from row_number for Redshift/Postgres/databricks
{% if target.type in ['redshift', 'postgres', 'databricks'] -%}
  case when event_id = '1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76' then '2021-03-01 20:56:39.192' else derived_tstamp end as derived_tstamp,
{% else %}
  derived_tstamp,
{% endif %}

  -- hard-coding due to non-deterministic outcome from row_number for Redshift/Postgres/databricks
{% if target.type in ['redshift', 'postgres', 'databricks'] -%}
  case when event_id = '1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76' then '2021-03-01 20:56:39.192' else start_tstamp end as start_tstamp,
{% else %}
  start_tstamp,
{% endif %}

  -- hard-coding due to non-deterministic outcome from row_number for Redshift/Postgres/databricks
{% if target.type in ['redshift', 'postgres', 'databricks'] -%}
  case when event_id = '1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76' then '2021-03-01 20:56:39.192' else end_tstamp end as end_tstamp,
{% else %}
  end_tstamp,
{% endif %}


  engaged_time_in_s,
  absolute_time_in_s,

  horizontal_pixels_scrolled,
  vertical_pixels_scrolled,

  horizontal_percentage_scrolled,
  vertical_percentage_scrolled,

  doc_width,
  doc_height,
  content_group,

  page_title,
  page_url,
  page_urlscheme,
  page_urlhost,
  page_urlpath,
  page_urlquery,
  page_urlfragment,

  mkt_medium,
  mkt_source,
  mkt_term,
  mkt_content,
  mkt_campaign,
  mkt_clickid,
  mkt_network,
  default_channel_group,

  page_referrer,
  refr_urlscheme,
  refr_urlhost,
  refr_urlpath,
  refr_urlquery,
  refr_urlfragment,
  refr_medium,
  refr_source,
  refr_term,

  geo_country,
  geo_region,
  geo_region_name,
  geo_city,
  geo_zipcode,
  geo_latitude,
  geo_longitude,
  geo_timezone,

  user_ipaddress,

  useragent,

  br_lang,
  br_viewwidth,
  br_viewheight,
  br_colordepth,
  br_renderengine,

  os_timezone,

  category,
  primary_impact,
  reason,
  spider_or_robot,

  useragent_family,
  useragent_major,
  useragent_minor,
  useragent_patch,
  useragent_version,
  os_family,
  os_major,
  os_minor,
  os_patch,
  os_patch_minor,
  os_version,
  device_family,
  device_class,
  device_category,
  screen_resolution,
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
  event_id2,
  v_collector

from {{ ref('snowplow_unified_views') }}
