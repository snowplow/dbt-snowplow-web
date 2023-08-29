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
    unique_key='consent_version',
    sort = 'version_start_tstamp',
    dist = 'consent_version',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}


{% if is_incremental() %}
{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(this,
                                                                          'last_allow_all_event',
                                                                          'last_allow_all_event') %}
{% endif %}

with consent_versions as (

  select
    consent_version,
    consent_scopes,
    consent_url,
    domains_applied,
    min(derived_tstamp) as version_start_tstamp,
    max(load_tstamp) as last_allow_all_event

  from {{ ref('snowplow_unified_consent_log') }}

  where event_name <> 'cmp_visible' and event_type = 'allow_all'

  {% if is_incremental() %} -- and it has not been processed yet
  and load_tstamp > {{ upper_limit }}
  {% endif %}

  group by 1,2,3,4
)

, latest_version as (

  select
    consent_version,
    version_start_tstamp

  from consent_versions

  order by 2 desc limit 1
)

{% if is_incremental() %}

select
  v.consent_version,
  least(v.version_start_tstamp, t.version_start_tstamp) as version_start_tstamp,
  v.consent_scopes,
  v.consent_url,
  v.domains_applied,
  case when l.consent_version is not null then True else False end is_latest_version,
  v.last_allow_all_event

from consent_versions v

left join latest_version l

on v.consent_version = l.consent_version

left join {{ this }} t
on t.consent_version = v.consent_version

{% else %}

select
  v.consent_version,
  v.version_start_tstamp,
  v.consent_scopes,
  v.consent_url,
  v.domains_applied,
  case when l.consent_version is not null then True else False end is_latest_version,
  v.last_allow_all_event

from consent_versions v

left join latest_version l

on v.consent_version = l.consent_version

{% endif %}
