select

  replace(cast(consent_version as {{ dbt.type_string() }}), '.0', '') ||'.0' as consent_version,
  cast(version_start_tstamp as {{ dbt.type_timestamp() }}) as version_start_tstamp,
  consent_scopes,
  consent_url,
  domains_applied,
  cast(is_latest_version as {{ dbt.type_boolean() }})as is_latest_version,
  cast(last_allow_all_event as {{ dbt.type_timestamp() }}) as last_allow_all_event,
  total_visitors,
  allow_all,
  allow_selected,
  allow,
  pending,
  denied,
  expired,
  withdrawn,
  implicit_consent,
  expires_in_six_months

from {{ ref('snowplow_web_consent_totals_expected') }}
