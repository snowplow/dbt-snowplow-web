{{
  config(
    materialized='snowplow_incremental',
    unique_key='event_id',
    upsert_date_key='derived_tstamp',
    partition_by = snowplow_utils.get_partition_by(databricks_partition_by = 'derived_tstamp_date'),
  )
}}

with prep as (

  select
    e.event_id,
    e.domain_userid,
    e.user_id,
    e.geo_country,
    e.page_view_id,
    e.domain_sessionid,
    e.derived_tstamp,
    e.load_tstamp,
    e.event_name,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1.event_type::STRING as event_type,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1.basis_for_processing::STRING as basis_for_processing,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1.consent_url::STRING as consent_url,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1.consent_version::STRING as consent_version,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1.consent_scopes::STRING as consent_scopes,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1.domains_applied::STRING as domains_applied,
    e.unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1.gdpr_applies::boolean as gdpr_applies,
    e.unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1.elapsed_time::float as cmp_load_time

  from {{ ref("snowplow_web_base_events_this_run") }} as e

  where event_name in ('cmp_visible', 'consent_preferences')

  and {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.

)

select
  p.event_id,
  p.domain_userid,
  p.user_id,
  p.geo_country,
  p.page_view_id,
  p.domain_sessionid,
  p.derived_tstamp,
  p.load_tstamp,
  p.event_name,
  p.event_type,
  p.basis_for_processing,
  p.consent_url,
  p.consent_version,
  replace(replace(replace(replace(p.consent_scopes, '"', ''), '[', ''), ']', ''), ',', ', ') as consent_scopes,
  replace(replace(replace(replace(p.domains_applied, '"', ''), '[', ''), ']', ''), ',', ', ') as domains_applied,
  coalesce(p.gdpr_applies, false) as gdpr_applies,
  p.cmp_load_time,
  date(derived_tstamp) as derived_tstamp_date

from prep p
