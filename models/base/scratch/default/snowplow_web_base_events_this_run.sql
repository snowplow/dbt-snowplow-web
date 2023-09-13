{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    sort='collector_tstamp',
    dist='event_id',
    tags=["this_run"]
  )
}}

{# dbt passed variables by reference so need to use copy to avoid altering the list multiple times #}
{% set contexts = var('snowplow__entities_or_sdes', []).copy() %}

{% do contexts.append({'schema': var('snowplow__page_view_context'), 'prefix': 'page_view', 'single_entity': True}) %}

{% if var('snowplow__enable_iab', false) -%}
  {% do contexts.append({'schema': var('snowplow__iab_context'), 'prefix': 'iab', 'single_entity': True}) %}
{% endif -%}

{% if var('snowplow__enable_ua', false) -%}
  {% do contexts.append({'schema': var('snowplow__ua_parser_context'), 'prefix': 'ua', 'single_entity': True}) %}
{% endif -%}

{% if var('snowplow__enable_yauaa', false) -%}
  {% do contexts.append({'schema': var('snowplow__yauaa_context'), 'prefix': 'yauaa', 'single_entity': True}) %}
{% endif -%}


{% if var('snowplow__enable_consent', false) -%}
  {% do contexts.append({'schema': var('snowplow__consent_cmp_visible'), 'prefix': 'cmp_visible', 'single_entity': True}) %}
  {% do contexts.append({'schema': var('snowplow__consent_preferences'), 'prefix': 'consent_pref', 'single_entity': True}) %}
{% endif -%}

{% if var('snowplow__enable_cwv', false) -%}
  {% do contexts.append({'schema': var('snowplow__cwv_context'), 'prefix': 'cwv', 'single_entity': True}) %}
{% endif -%}

{% set base_events_query = snowplow_utils.base_create_snowplow_events_this_run(
    sessions_this_run_table='snowplow_web_base_sessions_this_run',
    session_identifiers=var('snowplow__session_identifiers', [{"schema" : "atomic", "field" : "domain_sessionid"}]),
    session_sql=var('snowplow__session_sql', none),
    session_timestamp=var('snowplow__session_timestamp', 'collector_tstamp'),
    derived_tstamp_partitioned=var('snowplow__derived_tstamp_partitioned', true),
    days_late_allowed=var('snowplow__days_late_allowed', 3),
    max_session_days=var('snowplow__max_session_days', 3),
    app_ids=var('snowplow__app_ids', []),
    snowplow_events_database=var('snowplow__database', target.database) if target.type not in ['databricks', 'spark'] else var('snowplow__databricks_catalog', 'hive_metastore') if target.type in ['databricks'] else var('snowplow__atomic_schema', 'atomic'),
    snowplow_events_schema=var('snowplow__atomic_schema', 'atomic'),
    snowplow_events_table=var('snowplow__events_table', 'events'),
    entities_or_sdes=contexts) %}


with base_query as (
  {{ base_events_query }}
)

{% set base_query_cols = get_column_schema_from_query( 'select * from (' + base_events_query +') a') %}

select
  {% for col in base_query_cols | map(attribute='name') | list -%}
    {% if col == 'session_identifier' -%}
      a.session_identifier as domain_sessionid
    {%- elif col == 'domain_sessionid' -%}
      a.domain_sessionid as original_domain_sessionid
    {%- elif col == 'user_identifier' -%}
      a.user_identifier as domain_userid
    {%- elif col == 'domain_userid' -%}
      a.domain_userid as original_domain_userid
    {%- else -%}
      a.{{col}}
    {%- endif -%}
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}

from base_query a
