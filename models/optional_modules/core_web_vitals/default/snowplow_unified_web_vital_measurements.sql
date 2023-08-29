{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    materialized='table',
    enabled=var("snowplow__enable_cwv", false) and target.type in ('redshift', 'postgres') | as_bool(),
  )
}}

{% if target.type == 'redshift'%}
  {% set grouping_function = 'grouping_id' %}
{% else  %}
    {% set grouping_function = 'grouping' %}
{% endif %}

with prep as (

  select
    page_url,
    device_class,
    geo_country,
    concat(cast({{ date_trunc('day', 'derived_tstamp') }} as {{ type_string() }}),'.000') as time_period,
    lcp,
    fid,
    cls,
    ttfb,
    inp

  from {{ ref('snowplow_unified_web_vitals') }}

  where cast(derived_tstamp as date) >= {{ dateadd('day', '-'+var('snowplow__cwv_days_to_measure')|string, date_trunc('day', snowplow_utils.current_timestamp_in_utc())) }}

)

, lcp_measurements as (

  select
    {{ dbt_utils.generate_surrogate_key(['page_url', 'device_class', 'geo_country', 'time_period' ]) }} surrogate_key,
    page_url,
    device_class,
    geo_country,
    time_period,
    count(*) as page_view_count,
    {{ grouping_function }}(page_url, device_class) as id_url_and_device,
    {{ grouping_function }}(device_class) as id_device,
    {{ grouping_function }}(time_period) as id_period,
    {{ grouping_function }}(time_period, device_class) as id_period_and_device,
    {{ grouping_function }}(geo_country) as id_country,
    {{ grouping_function }}(geo_country, device_class) as id_country_and_device,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by lcp) as lcp_{{ var('snowplow__cwv_percentile') }}p

  from prep

  group by grouping sets ((), (page_url, device_class), (device_class), (time_period), (time_period, device_class), (geo_country), (geo_country, device_class))

)

, fid_measurements as (

  select
    {{ dbt_utils.generate_surrogate_key(['page_url', 'device_class', 'geo_country', 'time_period' ]) }} surrogate_key,
    page_url,
    device_class,
    geo_country,
    time_period,
    count(*) as page_view_count,
    {{ grouping_function }}(page_url, device_class) as id_url_and_device,
    {{ grouping_function }}(device_class) as id_device,
    {{ grouping_function }}(time_period) as id_period,
    {{ grouping_function }}(time_period, device_class) as id_period_and_device,
    {{ grouping_function }}(geo_country) as id_country,
    {{ grouping_function }}(geo_country, device_class) as id_country_and_device,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by fid) as fid_{{ var('snowplow__cwv_percentile') }}p

  from prep

  group by grouping sets ((), (page_url, device_class), (device_class), (time_period), (time_period, device_class), (geo_country), (geo_country, device_class))

)

, cls_measurements as (

  select
    {{ dbt_utils.generate_surrogate_key(['page_url', 'device_class', 'geo_country', 'time_period' ]) }} surrogate_key,
    page_url,
    device_class,
    geo_country,
    time_period,
    count(*) as page_view_count,
    {{ grouping_function }}(page_url, device_class) as id_url_and_device,
    {{ grouping_function }}(device_class) as id_device,
    {{ grouping_function }}(time_period) as id_period,
    {{ grouping_function }}(time_period, device_class) as id_period_and_device,
    {{ grouping_function }}(geo_country) as id_country,
    {{ grouping_function }}(geo_country, device_class) as id_country_and_device,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by cls) as cls_{{ var('snowplow__cwv_percentile') }}p

  from prep

  group by grouping sets ((), (page_url, device_class), (device_class), (time_period), (time_period, device_class), (geo_country), (geo_country, device_class))

)

, ttfb_measurements as (

  select
    {{ dbt_utils.generate_surrogate_key(['page_url', 'device_class', 'geo_country', 'time_period' ]) }} surrogate_key,
    page_url,
    device_class,
    geo_country,
    time_period,
    count(*) as page_view_count,
    {{ grouping_function }}(page_url, device_class) as id_url_and_device,
    {{ grouping_function }}(device_class) as id_device,
    {{ grouping_function }}(time_period) as id_period,
    {{ grouping_function }}(time_period, device_class) as id_period_and_device,
    {{ grouping_function }}(geo_country) as id_country,
    {{ grouping_function }}(geo_country, device_class) as id_country_and_device,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by ttfb) as ttfb_{{ var('snowplow__cwv_percentile') }}p

  from prep

  group by grouping sets ((), (page_url, device_class), (device_class), (time_period), (time_period, device_class), (geo_country), (geo_country, device_class))

)

, inp_measurements as (

  select
    {{ dbt_utils.generate_surrogate_key(['page_url', 'device_class', 'geo_country', 'time_period' ]) }} surrogate_key,
    page_url,
    device_class,
    geo_country,
    time_period,
    count(*) as page_view_count,
    {{ grouping_function }}(page_url, device_class) as id_url_and_device,
    {{ grouping_function }}(device_class) as id_device,
    {{ grouping_function }}(time_period) as id_period,
    {{ grouping_function }}(time_period, device_class) as id_period_and_device,
    {{ grouping_function }}(geo_country) as id_country,
    {{ grouping_function }}(geo_country, device_class) as id_country_and_device,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by inp) as inp_{{ var('snowplow__cwv_percentile') }}p

  from prep

  group by grouping sets ((), (page_url, device_class), (device_class), (time_period), (time_period, device_class), (geo_country), (geo_country, device_class))

)

, measurements as (

  select
    l.*,
    f.fid_{{ var('snowplow__cwv_percentile') }}p,
    c.cls_{{ var('snowplow__cwv_percentile') }}p,
    t.ttfb_{{ var('snowplow__cwv_percentile') }}p,
    i.inp_{{ var('snowplow__cwv_percentile') }}p

  from lcp_measurements l

  left join fid_measurements f on l.surrogate_key = f.surrogate_key

  left join cls_measurements c on l.surrogate_key = c.surrogate_key

  left join ttfb_measurements t on l.surrogate_key = t.surrogate_key

  left join inp_measurements i on l.surrogate_key = i.surrogate_key

)

, measurement_type as (

  select
    *,
    case when id_url_and_device <> 0 and id_device <> 0 and id_period <> 0 and id_period_and_device <> 0 and id_country <> 0 and id_country_and_device <> 0 then 'overall'
       when id_url_and_device = 0 then 'by_url_and_device'
       when id_period_and_device = 0 then 'by_day_and_device'
       when id_country_and_device = 0 then 'by_country_and_device'
       when id_country = 0 then 'by_country'
       when id_device = 0 then 'by_device'
       when id_period = 0 then 'by_day'
       end as measurement_type,
   {{ snowplow_unified.core_web_vital_results_query('_' + var('snowplow__cwv_percentile') | string + 'p') }}

  from measurements
)


, coalesce as (

  select
    measurement_type,
    coalesce(m.page_url, 'all') as page_url,
    coalesce(m.device_class, 'all') as device_class,
    coalesce(m.geo_country, 'all') as geo_country,
    coalesce(g.name, 'all') as country,
    coalesce(time_period, 'last {{var("snowplow__cwv_days_to_measure")|string }} days') as time_period,
    page_view_count,
    ceil(cast(lcp_{{ var('snowplow__cwv_percentile') }}p as decimal(14,4))*1000) /1000 as lcp_{{ var('snowplow__cwv_percentile') }}p,
    ceil(cast(fid_{{ var('snowplow__cwv_percentile') }}p as decimal(14,4))*1000) /1000 as fid_{{ var('snowplow__cwv_percentile') }}p,
    ceil(cast(cls_{{ var('snowplow__cwv_percentile') }}p as decimal(14,4))*1000) /1000 as cls_{{ var('snowplow__cwv_percentile') }}p,
    ceil(cast(ttfb_{{ var('snowplow__cwv_percentile') }}p as decimal(14,4))*1000) /1000 as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    ceil(cast(inp_{{ var('snowplow__cwv_percentile') }}p as decimal(14,4))*1000) /1000 as inp_{{ var('snowplow__cwv_percentile') }}p,
    m.lcp_result,
    m.fid_result,
    m.cls_result,
    m.ttfb_result,
    m.inp_result,
    {{ snowplow_unified.core_web_vital_pass_query() }} as passed

  from measurement_type m

  left join {{ ref(var('snowplow__geo_mapping_seed')) }} g on lower(m.geo_country) = lower(g.alpha_2)

  order by 1

)

select

  {{ dbt.concat(['page_url', "'-'" , 'device_class', "'-'" , 'geo_country', "'-'" , 'time_period' ]) }} compound_key,
  measurement_type,
  page_url,
  device_class,
  geo_country,
  country,
  time_period,
  page_view_count,
  lcp_{{ var('snowplow__cwv_percentile') }}p,
  fid_{{ var('snowplow__cwv_percentile') }}p,
  cls_{{ var('snowplow__cwv_percentile') }}p,
  ttfb_{{ var('snowplow__cwv_percentile') }}p,
  inp_{{ var('snowplow__cwv_percentile') }}p,
  lcp_result,
  fid_result,
  cls_result,
  ttfb_result,
  inp_result,
  passed

from coalesce
