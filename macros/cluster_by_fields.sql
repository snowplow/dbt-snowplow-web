{% macro web_cluster_by_fields_sessions_lifecycle() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_sessions_lifecycle', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_sessions_lifecycle() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["session_id"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro web_cluster_by_fields_page_views() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_page_views', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_page_views() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid","domain_sessionid"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro web_cluster_by_fields_sessions() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_sessions', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_sessions() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro web_cluster_by_fields_users() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_users', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_users() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["user_id","domain_userid"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}

{% macro web_cluster_by_fields_consent() %}

  {{ return(adapter.dispatch('web_cluster_by_fields_consent', 'snowplow_web')()) }}

{% endmacro %}

{% macro default__web_cluster_by_fields_consent() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["event_id","domain_userid"], snowflake_cols=["to_date(load_tstamp)"])) }}

{% endmacro %}
