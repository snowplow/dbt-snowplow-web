{{
  config(
    materialized='snowplow_incremental',
    unique_key='user_primary_key',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='user_primary_key',
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    }, databricks_partition_by='start_tstamp_date'),
    cluster_by=snowplow_web.web_cluster_by_fields_users(),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}


select distinct
  {{ dbt.concat(["u.domain_userid", "'-'", "s.user_ipaddress"]) }} as user_primary_key,
  u.*
  {% if target.type in ['databricks', 'spark'] -%}
  , DATE(start_tstamp) as start_tstamp_date
  {%- endif %}

from {{ ref('snowplow_web_users_this_run') }} u -- join sessions_this_run to sessions_conversion_this_run to produce complete sessions table
left join {{ ref('snowplow_web_sessions')}} s on u.domain_userid = s.domain_userid
where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
