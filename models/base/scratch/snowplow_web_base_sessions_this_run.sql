{{
    config(
      tags=["this_run"],
      post_hook=["{{ snowplow_utils.base_quarantine_sessions(var('snowplow__max_session_days', 3), var('snowplow__quarantined_sessions', 'snowplow_web_base_quarantined_sessions')) }}"],
      sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}


{% set sessions_query = snowplow_utils.base_create_snowplow_sessions_this_run(
    lifecycle_manifest_table='snowplow_web_base_sessions_lifecycle_manifest',
    new_event_limits_table='snowplow_web_base_new_event_limits') %}

{{ sessions_query }}
