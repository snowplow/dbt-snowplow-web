-- Removing model_tstamp

select
  -- app id
  app_id,

  -- session fields
  domain_sessionid,
  domain_sessionidx,

  start_tstamp,
  end_tstamp,

  -- user fields
  user_id,
  domain_userid,
  stitched_user_id,
  network_userid,

  -- engagement fields
  page_views,
  engaged_time_in_s,
  absolute_time_in_s,

  -- first page fields
  first_page_title,

  first_page_url,

  first_page_urlscheme,
  first_page_urlhost,
  first_page_urlpath,
  first_page_urlquery,
  first_page_urlfragment,

  last_page_title,

  last_page_url,

  last_page_urlscheme,
  last_page_urlhost,
  last_page_urlpath,
  last_page_urlquery,
  last_page_urlfragment,

  -- referrer fields
  referrer,

  refr_urlscheme,
  refr_urlhost,
  refr_urlpath,
  refr_urlquery,
  refr_urlfragment,

  refr_medium,
  refr_source,
  refr_term,

  -- marketing fields
  mkt_medium,
  mkt_source,
  mkt_term,
  mkt_content,
  mkt_campaign,
  mkt_clickid,
  mkt_network,

  -- geo fields
  geo_country,
  geo_region,
  geo_region_name,
  geo_city,
  geo_zipcode,
  geo_latitude,
  geo_longitude,
  geo_timezone,

  -- ip address
  user_ipaddress,

  -- user agent
  useragent,

  br_renderengine,
  br_lang,

  os_timezone,

  -- optional fields, only populated if in the page views module.

  -- iab enrichment fields
  category,
  primary_impact,
  reason,
  spider_or_robot,

  -- ua parser enrichment fields
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

  -- yauaa enrichment fields
  device_class,
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
  operating_system_version

from {{ ref('snowplow_web_sessions') }}
