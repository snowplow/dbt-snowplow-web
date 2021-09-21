{% macro cluster_by_fields_sessions_lifecycle() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["session_id"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro cluster_by_fields_page_views() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid","domain_sessionid"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro cluster_by_fields_sessions() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}


{% macro cluster_by_fields_users() %}

  {{ return(snowplow_utils.get_cluster_by(bigquery_cols=["user_id","domain_userid"], snowflake_cols=["to_date(start_tstamp)"])) }}

{% endmacro %}
