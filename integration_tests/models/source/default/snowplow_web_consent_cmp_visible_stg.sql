
select
  root_id,
  root_tstamp::timestamp,
  elapsed_time

from {{ ref('snowplow_web_consent_cmp_visible') }}
