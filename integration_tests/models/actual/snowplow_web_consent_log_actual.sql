{{
  config(
    enabled=var("snowplow__enable_consent", false)
    )
}}

select

  event_id,
  domain_userid,
  user_id,
  geo_country,
  page_view_id,
  domain_sessionid,
  derived_tstamp,
  load_tstamp,
  event_name,
  event_type,
  basis_for_processing,
  consent_url,
  consent_version,
  consent_scopes,
  domains_applied,
  gdpr_applies,
  cmp_load_time

from {{ ref('snowplow_web_consent_log') }}
