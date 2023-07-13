select
  cls,
  fcp,
  fid,
  inp,
  lcp,
  navigation_type,
  ttfb,
  root_tstamp::timestamp,
  root_id,
  schema_name

from {{ ref('snowplow_web_cwv_context') }}
