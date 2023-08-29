{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    materialized='incremental',
    enabled=var("snowplow__enable_consent", false),
    unique_key='domain_userid',
    sort = 'last_consent_event_tstamp',
    dist = 'domain_userid',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}


{% if is_incremental() %}
{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(this,
                                                                          'last_processed_event',
                                                                          'last_processed_event') %}
{% endif %}

with base as (

  select
    domain_userid,
    user_id,
    geo_country,
    max(load_tstamp) as last_processed_event,
    count(case when event_name = 'cmp_visible' then 1 end) as cmp_events,
    count(case when event_name = 'consent_preferences' then 1 end) as consent_events,
    max(case when event_name = 'cmp_visible' then derived_tstamp end) as last_cmp_event_tstamp,
    row_number() over(partition by domain_userid order by max(load_tstamp) desc) as latest_event_by_user_rank

  from {{ ref('snowplow_unified_consent_log') }}

  {% if is_incremental() %} -- and it has not been processed yet
  where load_tstamp > {{ upper_limit }}
  {% endif %}

  group by 1,2,3

)

, latest_consents as (

  select
    domain_userid,
    derived_tstamp as last_consent_event_tstamp,
    event_type as last_consent_event_type,
    consent_scopes as last_consent_scopes,
    consent_version as last_consent_version,
    consent_url as last_consent_url,
    domains_applied as last_domains_applied,
    row_number() over(partition by domain_userid order by load_tstamp desc) as latest_consent_event_by_user_rank

  from {{ ref('snowplow_unified_consent_log') }}

  where event_name = 'consent_preferences'

  {% if is_incremental() %} -- and it has not been processed yet
  and load_tstamp > {{ upper_limit }}
  {% endif %}

)

{% if is_incremental() %}

select
  b.domain_userid,
  b.user_id,
  b.geo_country,
  coalesce(b.cmp_events, 0) + coalesce(t.cmp_events, 0) as cmp_events,
  coalesce(b.consent_events, 0) + coalesce(t.consent_events, 0) as consent_events,
  b.last_cmp_event_tstamp,
  l.last_consent_event_tstamp,
  l.last_consent_event_type,
  l.last_consent_scopes,
  l.last_consent_version,
  l.last_consent_url,
  l.last_domains_applied,
  b.last_processed_event,
  case when v.is_latest_version then True else False end as is_latest_version

from base b

left join latest_consents l
on b.domain_userid = l.domain_userid

left join {{ ref('snowplow_unified_consent_versions')}} v
on v.consent_version = l.last_consent_version

left join {{ this }} t
on t.domain_userid = b.domain_userid

where (l.latest_consent_event_by_user_rank = 1 or l.domain_userid is null)
and b.latest_event_by_user_rank = 1

{% else %}

select
  b.domain_userid,
  b.user_id,
  b.geo_country,
  b.cmp_events,
  b.consent_events,
  b.last_cmp_event_tstamp,
  l.last_consent_event_tstamp,
  l.last_consent_event_type,
  l.last_consent_scopes,
  l.last_consent_version,
  l.last_consent_url,
  l.last_domains_applied,
  b.last_processed_event,
  case when v.is_latest_version then True else False end as is_latest_version

from base b

left join latest_consents l
on b.domain_userid = l.domain_userid

left join {{ ref('snowplow_unified_consent_versions') }} v
on v.consent_version = l.last_consent_version

where (l.latest_consent_event_by_user_rank = 1 or l.domain_userid is null)
and b.latest_event_by_user_rank = 1

{% endif %}
