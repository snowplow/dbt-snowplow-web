
select
  root_id,
  root_tstamp::timestamp,
  elapsed_time,
  'cmp_visible' as schema_name

from {{ ref('snowplow_web_consent_cmp_visible') }}
