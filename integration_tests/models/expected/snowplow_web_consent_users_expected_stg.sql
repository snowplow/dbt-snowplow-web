select

  domain_userid,
  user_id,
  geo_country,
  cmp_events,
  consent_events,
  cast(last_cmp_event_tstamp as {{ dbt.type_timestamp() }}) as last_cmp_event_tstamp,
  cast(last_consent_event_tstamp as {{ dbt.type_timestamp() }}) as last_consent_event_tstamp,
  last_consent_event_type,
  last_consent_scopes,
  replace(cast(last_consent_version as {{ dbt.type_string() }}), '.0', '') ||'.0' as last_consent_version,
  last_consent_url,
  last_domains_applied,
  cast(last_processed_event as {{ dbt.type_timestamp() }}) as last_processed_event,
  cast(is_latest_version as {{ dbt.type_boolean() }})  as is_latest_version

from {{ ref('snowplow_web_consent_users_expected') }}
