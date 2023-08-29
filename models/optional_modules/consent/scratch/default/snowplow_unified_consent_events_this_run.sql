{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    tags=["this_run"],
    enabled=var("snowplow__enable_consent", false) and target.type in ['redshift', 'postgres'] | as_bool(),
  )
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref('snowplow_unified_base_sessions_this_run'),
                                                                          'start_tstamp',
                                                                          'end_tstamp') %}

  select
    e.event_id,
    e.domain_userid,
    e.original_domain_userid,
    e.user_id,
    e.geo_country,
    e.page_view_id,
    e.domain_sessionid,
    e.original_domain_sessionid,
    e.derived_tstamp,
    e.load_tstamp,
    e.event_name,
    e.consent_pref_event_type as event_type,
    e.consent_pref_basis_for_processing as basis_for_processing,
    e.consent_pref_consent_url as consent_url,
    e.consent_pref_consent_version as consent_version,
    replace(translate(e.consent_pref_consent_scopes, '"[]', ''), ',', ', ') as consent_scopes,
    replace(translate(e.consent_pref_domains_applied, '"[]', ''), ',', ', ') as domains_applied,
    coalesce(e.consent_pref_gdpr_applies, false) as gdpr_applies,
    e.cmp_visible_elapsed_time as cmp_load_time

  from {{ ref("snowplow_unified_base_events_this_run") }} as e

  where event_name in ('cmp_visible', 'consent_preferences')

  and {{ snowplow_utils.is_run_with_new_events('snowplow_unified') }}

   --returns false if run doesn't contain new events.

  {% if var("snowplow__ua_bot_filter", false) %}
      {{ filter_bots() }}
  {% endif %}
