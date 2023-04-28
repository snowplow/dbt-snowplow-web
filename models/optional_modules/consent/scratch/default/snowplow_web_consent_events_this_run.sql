{{
  config(
    tags=["this_run"]
  )
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref('snowplow_web_base_sessions_this_run'),
                                                                          'start_tstamp',
                                                                          'end_tstamp') %}

with consent_pref as (

  select
    root_id,
    root_tstamp,
    event_type,
    basis_for_processing,
    consent_url,
    consent_version,
    consent_scopes,
    domains_applied,
    gdpr_applies,
    row_number() over (partition by root_id order by root_tstamp) dedupe_index

  from {{ var('snowplow__consent_preferences') }}

  where root_tstamp >= {{ lower_limit }}
  and root_tstamp <= {{ upper_limit }}

)

, cmp_visible  as (

  select
    root_id,
    root_tstamp,
    elapsed_time,
    row_number() over (partition by root_id order by root_tstamp) dedupe_index

  from {{ var('snowplow__consent_cmp_visible') }}

  where root_tstamp >= {{ lower_limit }}
  and root_tstamp <= {{ upper_limit }}

)

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
    replace(translate(p.consent_scopes, '"[]', ''), ',', ', ') as consent_scopes,
    replace(translate(p.domains_applied, '"[]', ''), ',', ', ') as domains_applied,
    coalesce(p.gdpr_applies, false) as gdpr_applies,
    v.elapsed_time as cmp_load_time

  from {{ ref("snowplow_web_base_events_this_run") }} as e

  left join consent_pref p
    on e.event_id = p.root_id
    and e.collector_tstamp = p.root_tstamp
    and p.dedupe_index = 1

  left join cmp_visible v
    on e.event_id = v.root_id
    and e.collector_tstamp = v.root_tstamp
    and v.dedupe_index = 1

  where event_name in ('cmp_visible', 'consent_preferences')

  and {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
