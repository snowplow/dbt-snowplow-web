select

  event_id,
  domain_userid,
  user_id,
  geo_country,
  cast(page_view_id as {{ dbt.type_string() }}) as page_view_id,
  domain_sessionid,
  cast(derived_tstamp as {{ dbt.type_timestamp() }}) as derived_timestamp,
  cast(load_tstamp as {{ dbt.type_timestamp() }}) as load_timestamp,
  event_name,
  event_type,
  basis_for_processing,
  consent_url,
  consent_version,
  consent_scopes,
  domains_applied,
  gdpr_applies,
  cmp_load_time

from {{ ref('snowplow_web_consent_log_expected') }}
