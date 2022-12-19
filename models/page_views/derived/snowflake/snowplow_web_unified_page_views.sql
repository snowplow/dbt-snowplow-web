{{
  config(
    materialized="view",
    tags=["derived"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with view_aggregations as (
  select
    page_view_id,
    event_id,

    app_id,

    -- user fields
    user_id,
    domain_userid,
    network_userid,

    -- session fields
    domain_sessionid as first_domain_sessionid,
    domain_sessionidx as first_domain_sessionidx,

    row_number() over (partition by domain_sessionid order by derived_tstamp) as page_view_in_session_index,
    count(distinct page_view_id) over (partition by domain_sessionid) as page_views_in_session,

    -- timestamp fields
    dvce_created_tstamp,
    collector_tstamp,
    derived_tstamp,
    start_tstamp,
    max(end_tstamp) over (partition by page_view_id) as end_tstamp,
    {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp,

    sum(engaged_time_in_s) over (partition by page_view_id) as engaged_time_in_s,
    sum(absolute_time_in_s) over (partition by page_view_id) as absolute_time_in_s,

    max(horizontal_pixels_scrolled) over (partition by page_view_id) as horizontal_pixels_scrolled,
    max(vertical_pixels_scrolled) over (partition by page_view_id) as vertical_pixels_scrolled,

    max(horizontal_percentage_scrolled) over (partition by page_view_id) as horizontal_percentage_scrolled,
    max(vertical_percentage_scrolled) over (partition by page_view_id) as vertical_percentage_scrolled,

    doc_width,
    doc_height,

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
    source

  from
  (select a.*, 'pv' as source from {{ ref('snowplow_web_page_views')}} a
  union all
  select a.* exclude(page_view_surrogate_id), 'spv' as source from {{ ref('snowplow_web_stray_page_views') }} a
  )
)

select
  a.* exclude(source)
from
  view_aggregations a
where
  source = 'pv'
