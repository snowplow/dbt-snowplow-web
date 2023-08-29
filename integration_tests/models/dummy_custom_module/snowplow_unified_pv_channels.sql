{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    materialized='incremental',
    enabled=var('snowplow__enable_custom_example'),
    unique_key='page_view_id',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='page_view_id',
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val={
      "field": "start_tstamp",
      "data_type": "timestamp"
    }),
    cluster_by=["page_view_id"],
    snowplow_optimize=true
  )
}}

select
  pv.page_view_id,
  pv.start_tstamp,
  -- Arbitary case statements and inefficient string search functions (for cross db compatibility). Do not copy.
  case
    when pv.refr_medium = 'search'
     and (lower(pv.mkt_medium) like '%cpc%' or lower(pv.mkt_source) like '%cpc%') then 'paidsearch'
    when lower(pv.mkt_medium) like '%paidsearch%'
     or  lower(pv.mkt_source) like '%paidsearch%' then 'paidsearch'
    when lower(pv.mkt_source) like '%adwords%'
     or  lower(pv.mkt_medium) like '%adwords%' then 'paidsearch'
    when lower(pv.mkt_source) like '%google%'
     and lower(pv.mkt_medium) like '%ads%' then 'paidsearch'
    when pv.refr_urlhost in ('www.googleadservices.com','googleads.g.doubleclick.net') then 'paidsearch'
    when lower(pv.mkt_medium) like '%cpv%' then 'advertising'
    when lower(pv.mkt_medium) like '%(display|cpm|banner)%' then 'display'
    when pv.refr_medium is null and pv.page_url not like '%utm_%' then 'direct'
    when (lower(pv.refr_medium) = 'search' and pv.mkt_medium is null)
     or (lower(pv.refr_medium) = 'search' and lower(pv.mkt_medium) = 'organic') then 'organicsearch'
    when pv.refr_medium = 'social' then 'social'
    when pv.refr_medium = 'unknown' or pv.mkt_medium = 'referral' then 'referral'
    when pv.refr_medium = 'internal' then 'internal'
    else 'others'
  end as channel

from {{ ref('snowplow_unified_views_this_run' ) }} pv
