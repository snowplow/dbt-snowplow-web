{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro web_cluster_by_fields_sessions_lifecycle() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_sessions_lifecycle', 'snowplow_unified')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_sessions_lifecycle() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["session_identifier"], snowflake_val=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro web_cluster_by_fields_page_views() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_page_views', 'snowplow_unified')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_page_views() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["domain_userid","domain_sessionid"], snowflake_val=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro web_cluster_by_fields_sessions() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_sessions', 'snowplow_unified')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_sessions() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["domain_userid"], snowflake_val=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro web_cluster_by_fields_users() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_users', 'snowplow_unified')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_users() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["user_id","domain_userid"], snowflake_val=["to_date(start_tstamp)"])) }}

{% endmacro %}

{% macro web_cluster_by_fields_consent() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_consent', 'snowplow_unified')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_consent() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["event_id","domain_userid"], snowflake_val=["to_date(load_tstamp)"])) }}

{% endmacro %}

{% macro web_cluster_by_fields_cwv() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_cwv', 'snowplow_unified')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_cwv() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["page_view_id","domain_userid"], snowflake_val=["to_date(derived_tstamp)"])) }}

{% endmacro %}
