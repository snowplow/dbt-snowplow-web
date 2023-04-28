{% macro web_cluster_by_fields_sessions_lifecycle() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_sessions_lifecycle', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_sessions_lifecycle() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["session_id"], snowflake_val=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro web_cluster_by_fields_page_views() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_page_views', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_page_views() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["domain_userid","domain_sessionid"], snowflake_val=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro web_cluster_by_fields_sessions() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_sessions', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_sessions() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["domain_userid"], snowflake_val=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro web_cluster_by_fields_users() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_users', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_users() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["user_id","domain_userid"], snowflake_val=["to_date(start_tstamp)"])) }}

{% endmacro %}

{% macro web_cluster_by_fields_consent() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_consent', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_consent() %}

  {{ return(snowplow_utils.get_value_by_target_type(bigquery_val=["event_id","domain_userid"], snowflake_val=["to_date(load_tstamp)"])) }}

{% endmacro %}
