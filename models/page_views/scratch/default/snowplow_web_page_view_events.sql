{{
  config(
    sort='start_tstamp',
    dist='page_view_id'
  )
}}

with page_view_events as (
  select
    ev.page_view_id,
    ev.event_id,

    ev.app_id,

    -- user fields
    ev.user_id,
    ev.domain_userid,
    ev.network_userid,

    -- session fields
    ev.domain_sessionid,
    ev.domain_sessionidx,

    -- timestamp fields
    ev.dvce_created_tstamp,
    ev.collector_tstamp,
    ev.derived_tstamp,
    ev.derived_tstamp as start_tstamp,

    ev.doc_width,
    ev.doc_height,

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

    ev.page_referrer,
    ev.refr_urlscheme ,
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
    ev.geo_timezone ,

    ev.user_ipaddress,

    ev.useragent,

    ev.br_lang,
    ev.br_viewwidth,
    ev.br_viewheight,
    ev.br_colordepth,
    ev.br_renderengine,
    ev.os_timezone,

    dense_rank() over (partition by ev.page_view_id order by ev.derived_tstamp) as page_view_id_dedupe_index

  from {{ ref('snowplow_web_base_events_this_run') }} as ev

  where ev.event_name = 'page_view'
  and ev.page_view_id is not null

  {% if var("snowplow__ua_bot_filter", true) %}
    {{ filter_bots() }}
  {% endif %}
)

-- Dedupe: Take first row of duplicate page view, unless derived_tstamp also duplicated.
-- Remove pv entirely if both fields are dupes. Avoids 1:many join with context tables.
, dedupe as (
  select
    *,
    count(*) over(partition by page_view_id) as row_count

  from page_view_events
  where page_view_id_dedupe_index = 1 -- Keep row(s) with earliest derived_tstamp per dupe pv
)

select
  pv.page_view_id,
  pv.event_id,

  pv.app_id,

  -- user fields
  pv.user_id,
  pv.domain_userid,
  pv.network_userid,

  -- session fields
  pv.domain_sessionid,
  pv.domain_sessionidx,

  -- timestamp fields
  pv.dvce_created_tstamp,
  pv.collector_tstamp,
  pv.derived_tstamp,
  pv.start_tstamp,

  pv.doc_width,
  pv.doc_height,

  pv.page_title,
  pv.page_url,
  pv.page_urlscheme,
  pv.page_urlhost,
  pv.page_urlpath,
  pv.page_urlquery,
  pv.page_urlfragment,

  pv.mkt_medium,
  pv.mkt_source,
  pv.mkt_term,
  pv.mkt_content,
  pv.mkt_campaign,
  pv.mkt_clickid,
  pv.mkt_network,

  pv.page_referrer,
  pv.refr_urlscheme ,
  pv.refr_urlhost,
  pv.refr_urlpath,
  pv.refr_urlquery,
  pv.refr_urlfragment,
  pv.refr_medium,
  pv.refr_source,
  pv.refr_term,

  pv.geo_country,
  pv.geo_region,
  pv.geo_region_name,
  pv.geo_city,
  pv.geo_zipcode,
  pv.geo_latitude,
  pv.geo_longitude,
  pv.geo_timezone ,

  pv.user_ipaddress,

  pv.useragent,

  pv.br_lang,
  pv.br_viewwidth,
  pv.br_viewheight,
  pv.br_colordepth,
  pv.br_renderengine,
  pv.os_timezone,

  row_number() over (partition by pv.domain_sessionid order by pv.derived_tstamp) as page_view_in_session_index --Moved to post dedupe, unlike V1 web model.

from dedupe as pv

where row_count = 1 -- Remove dupe page views with more than 1 row
