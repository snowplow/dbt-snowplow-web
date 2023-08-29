{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
    config(
      tags=["this_run"],
      post_hook=["{{ snowplow_utils.base_quarantine_sessions(var('snowplow__max_session_days', 3), var('snowplow__quarantined_sessions', 'snowplow_unified_base_quarantined_sessions')) }}"],
      sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
    )
}}


{% set sessions_query = snowplow_utils.base_create_snowplow_sessions_this_run(
    lifecycle_manifest_table='snowplow_unified_base_sessions_lifecycle_manifest',
    new_event_limits_table='snowplow_unified_base_new_event_limits') %}

{{ sessions_query }}
