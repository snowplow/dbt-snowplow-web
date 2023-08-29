{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    materialized='table',
    enabled=var("snowplow__enable_cwv", false) and target.type == 'bigquery' | as_bool()
  )
}}

with by_url_and_device as (

  select distinct

    page_url,
    device_class,
    'all' as geo_country,
    'last {{var("snowplow__cwv_days_to_measure")|string }} days' as time_period,
    count(*) over (partition by page_url, device_class) as page_view_count,
    percentile_cont(lcp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by page_url, device_class) as lcp_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(fid, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by page_url, device_class) as fid_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(cls, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by page_url, device_class) as cls_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(ttfb, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by page_url, device_class) as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(inp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by page_url, device_class) as inp_{{ var('snowplow__cwv_percentile') }}p,
    'by_url_and_device' as measurement_type

  from {{ ref('snowplow_unified_web_vitals') }}

  where cast(derived_tstamp as date) >= {{ dateadd('day', '-'+var('snowplow__cwv_days_to_measure')|string, date_trunc('day', snowplow_utils.current_timestamp_in_utc())) }}
)

, overall as (

  select distinct

    'all' as page_url,
    'all' as device_class,
    'all' as geo_country,
    'last {{var("snowplow__cwv_days_to_measure")|string }} days' as time_period,
    count(*) over() as page_view_count,
    percentile_cont(lcp, 0.{{ var('snowplow__cwv_percentile') }}) over() as lcp_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(fid, 0.{{ var('snowplow__cwv_percentile') }}) over() as fid_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(cls, 0.{{ var('snowplow__cwv_percentile') }}) over() as cls_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(ttfb, 0.{{ var('snowplow__cwv_percentile') }}) over() as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(inp, 0.{{ var('snowplow__cwv_percentile') }}) over() as inp_{{ var('snowplow__cwv_percentile') }}p,
    'overall' as measurement_type

  from {{ ref('snowplow_unified_web_vitals') }}

  where cast(derived_tstamp as date) >= {{ dateadd('day', '-'+var('snowplow__cwv_days_to_measure')|string, date_trunc('day', snowplow_utils.current_timestamp_in_utc())) }}
)

, by_device as (

  select distinct

    'all' as page_url,
    device_class,
    'all' as geo_country,
    'last {{var("snowplow__cwv_days_to_measure")|string }} days' as time_period,
    count(*) over (partition by device_class) as page_view_count,
    percentile_cont(lcp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by device_class) as lcp_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(fid, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by device_class) as fid_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(cls, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by device_class) as cls_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(ttfb, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by device_class) as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(inp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by device_class) as inp_{{ var('snowplow__cwv_percentile') }}p,
    'by_device' as measurement_type

  from {{ ref('snowplow_unified_web_vitals') }}

  where cast(derived_tstamp as date) >= {{ dateadd('day', '-'+var('snowplow__cwv_days_to_measure')|string, date_trunc('day', snowplow_utils.current_timestamp_in_utc())) }}
)

, by_day as (

  select distinct

    'all' as page_url,
    'all' as device_class,
    'all' as geo_country,
    cast( {{ dbt.date_trunc('day', 'derived_tstamp') }} as {{ dbt.type_string() }}) as time_period,
    count(*) over (partition by cast( {{ dbt.date_trunc('day', 'derived_tstamp') }} as {{ dbt.type_string() }})) as page_view_count,
    percentile_cont(lcp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}) as lcp_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(fid, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}) as fid_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(cls, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}) as cls_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(ttfb, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}) as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(inp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}) as inp_{{ var('snowplow__cwv_percentile') }}p,
    'by_day' as measurement_type

  from {{ ref('snowplow_unified_web_vitals') }}

  where cast(derived_tstamp as date) >= {{ dateadd('day', '-'+var('snowplow__cwv_days_to_measure')|string, date_trunc('day', snowplow_utils.current_timestamp_in_utc())) }}

)

, by_day_and_device as (

  select distinct

    'all' as page_url,
    device_class,
    'all' as geo_country,
    cast( {{ dbt.date_trunc('day', 'derived_tstamp') }} as {{ dbt.type_string() }}) as time_period,
    count(*) over (partition by device_class, cast( {{ dbt.date_trunc('day', 'derived_tstamp') }} as {{ dbt.type_string() }})) as page_view_count,
    percentile_cont(lcp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}, device_class) as lcp_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(fid, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}, device_class) as fid_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(cls, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}, device_class) as cls_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(ttfb, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}, device_class) as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(inp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by {{ dbt.date_trunc('day', 'derived_tstamp') }}, device_class) as inp_{{ var('snowplow__cwv_percentile') }}p,
    'by_day_and_device' as measurement_type

  from {{ ref('snowplow_unified_web_vitals') }}

  where cast(derived_tstamp as date) >= {{ dateadd('day', '-'+var('snowplow__cwv_days_to_measure')|string, date_trunc('day', snowplow_utils.current_timestamp_in_utc())) }}

)

, by_country as (

  select distinct

    'all' as page_url,
    'all' as device_class,
    geo_country,
    'last {{var("snowplow__cwv_days_to_measure")|string }} days' as time_period,
    count(*) over (partition by geo_country) as page_view_count,
    percentile_cont(lcp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country) as lcp_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(fid, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country) as fid_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(cls, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country) as cls_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(ttfb, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country) as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(inp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country) as inp_{{ var('snowplow__cwv_percentile') }}p,
    'by_country' as measurement_type

  from {{ ref('snowplow_unified_web_vitals') }}

  where cast(derived_tstamp as date) >= {{ dateadd('day', '-'+var('snowplow__cwv_days_to_measure')|string, date_trunc('day', snowplow_utils.current_timestamp_in_utc())) }}

)

, by_country_and_device as (

  select distinct

    'all' as page_url,
    device_class,
    geo_country,
    'last {{var("snowplow__cwv_days_to_measure")|string }} days' as time_period,
    count(*) over (partition by geo_country, device_class) as page_view_count,
    percentile_cont(lcp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country, device_class) as lcp_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(fid, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country, device_class) as fid_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(cls, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country, device_class) as cls_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(ttfb, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country, device_class) as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(inp, 0.{{ var('snowplow__cwv_percentile') }}) over (partition by geo_country, device_class) as inp_{{ var('snowplow__cwv_percentile') }}p,
    'by_country_and_device' as measurement_type

  from {{ ref('snowplow_unified_web_vitals') }}

  where cast(derived_tstamp as date) >= {{ dateadd('day', '-'+var('snowplow__cwv_days_to_measure')|string, date_trunc('day', snowplow_utils.current_timestamp_in_utc())) }}

)

, measurements as (

  select *,

  {{ snowplow_unified.core_web_vital_results_query('_' + var('snowplow__cwv_percentile') | string + 'p') }}

  from by_url_and_device

  union all

  select *,

  {{ snowplow_unified.core_web_vital_results_query('_' + var('snowplow__cwv_percentile') | string + 'p') }}

  from overall

  union all

  select *,

  {{ snowplow_unified.core_web_vital_results_query('_' + var('snowplow__cwv_percentile') | string + 'p') }}

  from by_device

  union all

  select *,

  {{ snowplow_unified.core_web_vital_results_query('_' + var('snowplow__cwv_percentile') | string + 'p') }}

  from by_day

  union all

   select *,

  {{ snowplow_unified.core_web_vital_results_query('_' + var('snowplow__cwv_percentile') | string + 'p') }}

  from by_day_and_device

  union all

  select *,

  {{ snowplow_unified.core_web_vital_results_query('_' + var('snowplow__cwv_percentile') | string + 'p') }}

  from by_country

  union all

  select *,

  {{ snowplow_unified.core_web_vital_results_query('_' + var('snowplow__cwv_percentile') | string + 'p') }}

  from by_country_and_device

)

, coalesce as (

select
  m.measurement_type,
  m.page_url,
  m.device_class,
  m.geo_country,
  coalesce(g.name, 'all') as country,
  m.time_period,
  m.page_view_count,
  ceil(cast(m.lcp_{{ var('snowplow__cwv_percentile') }}p as decimal) * 1000) /1000 as lcp_{{ var('snowplow__cwv_percentile') }}p,
  ceil(cast(m.fid_{{ var('snowplow__cwv_percentile') }}p as decimal) * 1000) /1000 as fid_{{ var('snowplow__cwv_percentile') }}p,
  ceil(cast(m.cls_{{ var('snowplow__cwv_percentile') }}p as decimal) * 1000) /1000 as cls_{{ var('snowplow__cwv_percentile') }}p,
  ceil(cast(m.ttfb_{{ var('snowplow__cwv_percentile') }}p as decimal) * 1000) /1000 as ttfb_{{ var('snowplow__cwv_percentile') }}p,
  ceil(cast(m.inp_{{ var('snowplow__cwv_percentile') }}p as decimal) * 1000) /1000 as inp_{{ var('snowplow__cwv_percentile') }}p,
  m.lcp_result,
  m.fid_result,
  m.cls_result,
  m.ttfb_result,
  m.inp_result,
  {{ snowplow_unified.core_web_vital_pass_query() }} as passed

from measurements m

left join {{ ref(var('snowplow__geo_mapping_seed')) }} g on lower(m.geo_country) = lower(g.alpha_2)

order by 1

)

select

  {{ dbt.concat(['page_url', "'-'" , 'device_class', "'-'" , 'geo_country', "'-'" , 'time_period' ]) }} compound_key,
  *

from coalesce
