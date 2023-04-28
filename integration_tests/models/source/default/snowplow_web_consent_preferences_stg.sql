
select
  root_id,
  root_tstamp::timestamp as root_tstamp,
  basis_for_processing,
  consent_version,
  consent_scopes,
  domains_applied,
  consent_url,
  event_type,
  gdpr_applies::boolean as gdpr_applies

from {{ ref('snowplow_web_consent_preferences') }}
