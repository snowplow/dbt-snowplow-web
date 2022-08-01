{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='domain_sessionid',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='domain_sessionid',
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    }, databricks_partition_by='start_tstamp_date'),
    cluster_by=snowplow_web.web_cluster_by_fields_sessions(),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  ) 
}}


select 
  s.*,
  {% if target.type in ['databricks', 'spark'] -%}
  , DATE(start_tstamp) as start_tstamp_date
  {%- endif %}
  c.is_session_w_intent,
  c.is_session_w_conversion

from {{ ref('snowplow_web_sessions_this_run') }} s -- join sessions_this_run to sessions_conversion_this_run to produce complete sessions table
left join {{ ref('snowplow_web_sessions_conversion_this_run')}} c
on s.domain_sessionid = c.domain_sessionid

where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
