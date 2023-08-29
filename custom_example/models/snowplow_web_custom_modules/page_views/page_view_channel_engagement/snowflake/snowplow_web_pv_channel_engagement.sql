{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

--Using `snowplow_optimize` config to reduce table scans. Could also use the standard `incremental` materialization.

{{
  config(
    materialized='incremental',
    unique_key='page_view_id',
    upsert_date_key='start_tstamp',
    cluster_by=snowplow_web.web_cluster_by_fields_page_views(),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    snowplow_optimize=true
  )
}}

with link_clicks as (
  select distinct
    ev.page_view_id,

    count(ev.event_id)
      over(partition by ev.page_view_id
      order by ev.derived_tstamp desc
      rows between unbounded preceding and unbounded following)
      as link_clicks,

    first_value(ev.unstruct_event_com_snowplowanalytics_snowplow_link_click_1:targetUrl::varchar)
      over(partition by ev.page_view_id
      order by ev.derived_tstamp desc
      rows between unbounded preceding and unbounded following)
      as first_link_target

  from {{ ref('snowplow_web_base_events_this_run' ) }} ev -- Select events from base_events_this_run rather than raw events table

  where
    {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
    and ev.unstruct_event_com_snowplowanalytics_snowplow_link_click_1 is not null -- only include link click events
)

, engagement as (
  select
    pv.page_view_id,
    pv.start_tstamp,
    case
      when pv.refr_medium = 'search'
       and (rlike(lower(pv.mkt_medium), '(cpc|ppc|sem|paidsearch)')
         or rlike(lower(pv.mkt_source), '(cpc|ppc|sem|paidsearch)')) then 'paidsearch'
      when pv.mkt_medium ilike '%paidsearch%'
       or pv.mkt_source ilike '%paidsearch%' then 'paidsearch'
      when rlike(lower(pv.mkt_source), '(adwords|google_paid|googleads)')
       or rlike(lower(pv.mkt_medium), '(adwords|google_paid|googleads)') then 'paidsearch'
      when pv.mkt_source ilike '%google%'
       and pv.mkt_medium ilike '%ads%' then 'paidsearch'
      when pv.refr_urlhost in ('www.googleadservices.com','googleads.g.doubleclick.net') then 'paidsearch'
      when rlike(lower(pv.mkt_medium), '(cpv|cpa|cpp|content-text|advertising|ads)') then 'advertising'
      when rlike(lower(pv.mkt_medium), '(display|cpm|banner)') then 'display'
      when pv.refr_medium is null and pv.page_url not ilike '%utm_%' then 'direct'
      when (lower(pv.refr_medium) = 'search' and pv.mkt_medium is null)
       or (lower(pv.refr_medium) = 'search' and lower(pv.mkt_medium) = 'organic') then 'organicsearch'
      when pv.refr_medium = 'social'
       or regexp_count(lower(pv.mkt_source),'^((.*(facebook|linkedin|instagram|insta|slideshare|social|tweet|twitter|youtube|lnkd|pinterest|googleplus|instagram|plus.google.com|quora|reddit|t.co|twitch|viadeo|xing|youtube).*)|(yt|fb|li))$')>0
       or regexp_count(lower(pv.mkt_medium),'^(.*)(social|facebook|linkedin|twitter|instagram|tweet)(.*)$')>0 then 'social'
      when pv.refr_medium = 'email'
       or lower(pv.mkt_medium) = '_mail' then 'email'
      when lower(pv.mkt_medium) = 'affiliate' then 'affiliate'
      when pv.refr_medium = 'unknown' or lower(pv.mkt_medium) = 'referral' or lower(pv.mkt_medium) = 'referal' then 'referral'
      when pv.refr_medium = 'internal' then 'internal'
      else 'others'
    end as channel,
    case
      when pv.engaged_time_in_s = 0 then true
      else false
    end as is_bounced_page_view,
    (pv.vertical_percentage_scrolled / 100) * 0.3 + (pv.engaged_time_in_s / 600) * 0.7 as engagement_score

  from {{ ref('snowplow_web_page_views_this_run' ) }} pv --select from page_views_this_run rather than derived page_views table
  where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
)

select
  eng.page_view_id,
  eng.start_tstamp,
  lc.link_clicks,
  lc.first_link_target,
  eng.is_bounced_page_view,
  eng.engagement_score,
  eng.channel

from engagement eng
left join link_clicks lc
on eng.page_view_id = lc.page_view_id
