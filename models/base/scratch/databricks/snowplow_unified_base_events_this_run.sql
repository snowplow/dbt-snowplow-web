{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    tags=["this_run"]
  )
}}

{{ config_check() }}

{% set base_events_query = snowplow_utils.base_create_snowplow_events_this_run(
    sessions_this_run_table='snowplow_unified_base_sessions_this_run',
    session_identifiers=var('snowplow__session_identifiers', [{"table" : "events", "field" : "domain_sessionid"}]),
    session_sql=var('snowplow__session_sql', none),
    session_timestamp=var('snowplow__session_timestamp', 'collector_tstamp'),
    derived_tstamp_partitioned=var('snowplow__derived_tstamp_partitioned', true),
    days_late_allowed=var('snowplow__days_late_allowed', 3),
    max_session_days=var('snowplow__max_session_days', 3),
    app_ids=var('snowplow__app_ids', []),
    snowplow_events_database=var('snowplow__database', target.database) if target.type not in ['databricks', 'spark'] else var('snowplow__databricks_catalog', 'hive_metastore') if target.type in ['databricks'] else var('snowplow__atomic_schema', 'atomic'),
    snowplow_events_schema=var('snowplow__atomic_schema', 'atomic'),
    snowplow_events_table=var('snowplow__events_table', 'events')) %}


with base_query as (
  {{ base_events_query }}
)

select
  coalesce(
    {% if var('snowplow__enable_web') %}
      a.contexts_com_snowplowanalytics_snowplow_web_page_1[0].id,
    {% endif %}
    {% if var('snowplow__enable_mobile') %}
      a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1.id::STRING,
    {% endif %}
    null) as view_id,

    coalesce(
    {% if var('snowplow__enable_web') %}
      'page_view',
    {% endif %}
    {% if var('snowplow__enable_mobile') %}
      'screen_view',
    {% endif %}
    null) as view_type,

    -- only adding there for the int tests to pass before I start the pageview unification feature
    {% if var('snowplow__enable_web') %}
    a.contexts_com_snowplowanalytics_snowplow_web_page_1[0].id as page_view_id,
    {% endif %}
    {% if var('snowplow__enable_mobile') %}
    a.unstruct_event_com_snowplowanalytics_mobile_screen_view_1.id::STRING as screen_view_id,
    {% endif %}
    a.domain_sessionid as original_domain_sessionid,
    a.domain_userid as original_domain_userid,

{% set base_query_cols = get_column_schema_from_query( 'select * from (' + base_events_query +') a') %}
   {% for col in base_query_cols | map(attribute='name') | list -%}
    {% if col == 'domain_userid' -%}
      a.user_identifier as domain_userid
    {% else %}
    a.{{col}}
    {% endif %}
    {%- if not loop.last -%},{%- endif %}
  {% endfor %}

from base_query a
