select

  event_id,
  domain_userid,
  cast(page_view_id as {{ dbt.type_string() }}) as page_view_id,
  domain_sessionid,
  cmp_load_time,
  cast(cmp_tstamp as {{ dbt.type_timestamp() }}) as cmp_tstamp,
  cast(first_consent_event_tstamp as {{ dbt.type_timestamp() }}) as first_consent_event_tstamp,
  first_consent_event_type,
  cmp_interaction_time

from {{ ref('snowplow_web_consent_cmp_stats_expected') }}
