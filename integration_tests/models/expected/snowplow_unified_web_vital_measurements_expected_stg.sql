{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

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

from {{ ref('snowplow_unified_web_vital_measurements_expected') }}
