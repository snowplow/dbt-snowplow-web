{{ 
  config(
    materialized='snowplow_incremental',
    enabled=var('snowplow__enable_custom_example'),
    unique_key='page_view_id',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='page_view_id'
  ) 
}}

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
  end as channel

from {{ ref('snowplow_web_page_views_this_run' ) }} pv 
