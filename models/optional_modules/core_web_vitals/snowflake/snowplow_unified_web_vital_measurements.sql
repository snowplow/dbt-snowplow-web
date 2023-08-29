{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    materialized='table',
    enabled=var("snowplow__enable_cwv", false) and target.type == 'snowflake' | as_bool(),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with measurements as (

  select
    page_url,
    device_class,
    geo_country,
    cast( {{ dbt.date_trunc('day', 'derived_tstamp') }} as {{ dbt.type_string() }}) as time_period,
    count(*) as page_view_count,
    grouping_id(page_url, device_class) as id_url_and_device,
    grouping_id(device_class) as id_device,
    grouping_id(cast( {{ dbt.date_trunc('day', 'derived_tstamp') }} as {{ dbt.type_string() }})) as id_period,
    grouping_id(cast( {{ dbt.date_trunc('day', 'derived_tstamp') }} as {{ dbt.type_string() }}), device_class) as id_period_and_device,
    grouping_id(geo_country) as id_country,
    grouping_id(geo_country, device_class) as id_country_and_device,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by lcp) as lcp_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by fid) as fid_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by cls) as cls_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by ttfb) as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    percentile_cont(0.{{ var('snowplow__cwv_percentile') }}) within group (order by inp) as inp_{{ var('snowplow__cwv_percentile') }}p

  from {{ ref('snowplow_unified_web_vitals') }}

  where cast(derived_tstamp as date) >= {{ dateadd('day', '-'+var('snowplow__cwv_days_to_measure')|string, date_trunc('day', snowplow_utils.current_timestamp_in_utc())) }}

  group by grouping sets ((), (page_url, device_class), (device_class), (cast( {{ dbt.date_trunc('day', 'derived_tstamp') }} as {{ dbt.type_string() }})), (cast( {{ dbt.date_trunc('day', 'derived_tstamp') }} as {{ dbt.type_string() }}), device_class), (geo_country), (geo_country, device_class))

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
    m.measurement_type,
    coalesce(m.page_url, 'all') as page_url,
    coalesce(m.device_class, 'all') as device_class,
    coalesce(m.geo_country, 'all') as geo_country,
    coalesce(g.name, 'all') as country,
    coalesce(time_period, 'last {{var("snowplow__cwv_days_to_measure")|string }} days') as time_period,
    page_view_count,
    ceil(lcp_{{ var('snowplow__cwv_percentile') }}p, 3) as lcp_{{ var('snowplow__cwv_percentile') }}p,
    ceil(fid_{{ var('snowplow__cwv_percentile') }}p, 3) as fid_{{ var('snowplow__cwv_percentile') }}p,
    ceil(cls_{{ var('snowplow__cwv_percentile') }}p, 3) as cls_{{ var('snowplow__cwv_percentile') }}p,
    ceil(ttfb_{{ var('snowplow__cwv_percentile') }}p, 3) as ttfb_{{ var('snowplow__cwv_percentile') }}p,
    ceil(inp_{{ var('snowplow__cwv_percentile') }}p, 3) as inp_{{ var('snowplow__cwv_percentile') }}p,
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
  *
from coalesce
