{{
  config(
    tags=["this_run"]
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
    p.event_type,
    p.basis_for_processing,
    p.consent_url,
    p.consent_version,
    p.consent_scopes,
    p.domains_applied,
    p.gdpr_applies,
    v.elapsed_time as cmp_load_time

  from {{ ref("snowplow_web_base_events_this_run") }} as e

  left join {{ var('snowplow__consent_preferences') }} p
    on e.event_id = p.root_id
    and e.collector_tstamp = p.root_tstamp

  left join {{ var('snowplow__consent_cmp_visible') }} v
    on e.event_id = v.root_id
    and e.collector_tstamp = v.root_tstamp

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
  p.cmp_load_time

from prep p
