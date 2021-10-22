{{ 
  config(
    materialized='table',
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid"]),
    sort='start_tstamp',
    dist='domain_sessionid',
    tags=["this_run"]
  ) 
}}


select
  -- app id
  a.app_id,

  -- session fields
  a.domain_sessionid,
  a.domain_sessionidx,

  b.start_tstamp,
  b.end_tstamp,
  {{ dbt_utils.current_timestamp_in_utc() }} as model_tstamp,

  -- user fields
  a.user_id,
  a.domain_userid,

  {% if var('snowplow__session_stitching') %}
    -- updated with mapping as part of post hook on derived sessions table
    a.domain_userid as stitched_user_id, 
  {% else %}
    cast(null as {{ dbt_utils.type_string() }}) as stitched_user_id,
  {% endif %}
  
  a.network_userid,

  -- engagement fields
  b.page_views,
  b.engaged_time_in_s,
  {{ snowplow_utils.timestamp_diff('b.start_tstamp', 'b.end_tstamp', 'second') }} as absolute_time_in_s,

  -- first page fields
  a.page_title as first_page_title,

  a.page_url as first_page_url,

  a.page_urlscheme as first_page_urlscheme,
  a.page_urlhost as first_page_urlhost,
  a.page_urlpath as first_page_urlpath,
  a.page_urlquery as first_page_urlquery,
  a.page_urlfragment as first_page_urlfragment,

  c.last_page_title,

  c.last_page_url,

  c.last_page_urlscheme,
  c.last_page_urlhost,
  c.last_page_urlpath,
  c.last_page_urlquery,
  c.last_page_urlfragment,

  -- referrer fields
  a.page_referrer as referrer,

  a.refr_urlscheme,
  a.refr_urlhost,
  a.refr_urlpath,
  a.refr_urlquery,
  a.refr_urlfragment,

  a.refr_medium,
  a.refr_source,
  a.refr_term,

  -- marketing fields
  a.mkt_medium,
  a.mkt_source,
  a.mkt_term,
  a.mkt_content,
  a.mkt_campaign,
  a.mkt_clickid,
  a.mkt_network,

  -- geo fields
  a.geo_country,
  a.geo_region,
  a.geo_region_name,
  a.geo_city,
  a.geo_zipcode,
  a.geo_latitude,
  a.geo_longitude,
  a.geo_timezone,

  -- ip address
  a.user_ipaddress,

  -- user agent
  a.useragent,

  a.br_renderengine,
  a.br_lang,

  a.os_timezone,

  -- optional fields, only populated if enabled.

  -- iab enrichment fields
  a.category,
  a.primary_impact,
  a.reason,
  a.spider_or_robot,

  -- ua parser enrichment fields
  a.useragent_family,
  a.useragent_major,
  a.useragent_minor,
  a.useragent_patch,
  a.useragent_version,
  a.os_family,
  a.os_major,
  a.os_minor,
  a.os_patch,
  a.os_patch_minor,
  a.os_version,
  a.device_family,

  -- yauaa enrichment fields
  a.device_class,
  a.agent_class,
  a.agent_name,
  a.agent_name_version,
  a.agent_name_version_major,
  a.agent_version,
  a.agent_version_major,
  a.device_brand,
  a.device_name,
  a.device_version,
  a.layout_engine_class,
  a.layout_engine_name,
  a.layout_engine_name_version,
  a.layout_engine_name_version_major,
  a.layout_engine_version,
  a.layout_engine_version_major,
  a.operating_system_class,
  a.operating_system_name,
  a.operating_system_name_version,
  a.operating_system_version

from {{ ref('snowplow_web_sessions_aggs') }} as b

inner join {{ ref('snowplow_web_page_views_this_run') }} as a
on a.domain_sessionid = b.domain_sessionid
and a.start_tstamp = b.start_tstamp
and a.page_view_in_session_index = 1

inner join {{ ref('snowplow_web_sessions_lasts') }} c
on b.domain_sessionid = c.domain_sessionid
