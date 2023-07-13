{{
  config(
    enabled=var("snowplow__enable_cwv", false) | as_bool()
    )
}}

select
  compound_key,
  measurement_type,
  page_url,
  device_class,
  geo_country,
  country,
  time_period,
  page_view_count,
  lcp_75p,
  fid_75p,
  cls_75p,
  ttfb_75p,
  inp_75p,
  lcp_result,
  fid_result,
  cls_result,
  ttfb_result,
  inp_result,
  passed

from {{ ref('snowplow_web_vital_measurements_expected') }}
