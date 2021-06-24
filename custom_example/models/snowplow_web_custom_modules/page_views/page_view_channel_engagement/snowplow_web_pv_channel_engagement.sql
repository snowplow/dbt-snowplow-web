--Using `snowplow_incremental` materialization to reduce table scans. Could also use the standard `incremental` materialization.

{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='page_view_id',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='page_view_id'
  ) 
}}

with link_clicks as (
  select
    ev.page_view_id,

    count(ev.event_id)
      over(partition by ev.page_view_id
      order by ev.derived_tstamp desc
      rows between unbounded preceding and unbounded following)
      as link_clicks,

    first_value(lc.target_url)
      over(partition by ev.page_view_id
      order by ev.derived_tstamp desc
      rows between unbounded preceding and unbounded following)
      as first_link_target

  from {{ source('atomic','com_snowplowanalytics_snowplow_link_click_1') }} lc

  inner join {{ ref('snowplow_web_base_events_this_run' ) }} ev -- Select events from base_events_this_run rather than raw events table
  on lc.root_id = ev.event_id and lc.root_tstamp = ev.collector_tstamp

  where
    lc.root_tstamp >= (select lower_limit from {{ ref('snowplow_web_base_new_event_limits') }}) -- limit link clicks table scan using the base_new_event_limits table
    and lc.root_tstamp <= (select upper_limit from {{ ref('snowplow_web_base_new_event_limits') }})
    and {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
)

, engagement as (
  select
    pv.page_view_id,
    pv.start_tstamp,
    case
      when pv.refr_medium = 'search'
       and (lower(pv.mkt_medium) similar to '%(cpc|ppc|sem|paidsearch)%'
         or lower(pv.mkt_source) similar to '%(cpc|ppc|sem|paidsearch)%') then 'paidsearch'
      when lower(pv.mkt_medium) ilike '%paidsearch%'
       or lower(pv.mkt_source) ilike '%paidsearch%' then 'paidsearch'
      when lower(pv.mkt_source) similar to '%(adwords|google_paid|googleads)%'
       or lower(pv.mkt_medium) similar to '%(adwords|google_paid|googleads)%' then 'paidsearch'
      when pv.mkt_source ilike '%google%'
       and pv.mkt_medium ilike '%ads%' then 'paidsearch'
      when pv.refr_urlhost in ('www.googleadservices.com','googleads.g.doubleclick.net') then 'paidsearch'
      when lower(pv.mkt_medium) similar to '%(cpv|cpa|cpp|content-text|advertising|ads)%' then 'advertising'
      when lower(pv.mkt_medium) similar to '%(display|cpm|banner)%' then 'display'
      when pv.refr_medium is null and pv.page_url not ilike '%utm_%' then 'direct'
      when (lower(pv.refr_medium) = 'search' and pv.mkt_medium is null)
       or (lower(pv.refr_medium) = 'search' and lower(pv.mkt_medium) = 'organic') then 'organicsearch'
      when pv.refr_medium = 'social'
       or regexp_count(lower(pv.mkt_source),'^((.*(facebook|linkedin|instagram|insta|slideshare|social|tweet|twitter|youtube|lnkd|pinterest|googleplus|instagram|plus.google.com|quora|reddit|t.co|twitch|viadeo|xing|youtube).*)|(yt|fb|li))$')>0
       or regexp_count(lower(pv.mkt_medium),'^(.*)(social|facebook|linkedin|twitter|instagram|tweet)(.*)$')>0 then 'social'
      when pv.refr_medium = 'email'
       or pv.mkt_medium ilike '_mail' then 'email'
      when pv.mkt_medium ilike 'affiliate' then 'affiliate'
      when pv.refr_medium = 'unknown' or lower(pv.mkt_medium) ilike 'referral' or lower(pv.mkt_medium) ilike 'referal' then 'referral'
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
  eng.engagement_score

from engagement eng
left join link_clicks lc
on eng.page_view_id = lc.page_view_id
