{{
  config(
    materialized= 'incremental',
    unique_key='event_id',
    upsert_date_key='derived_tstamp',
    sort='derived_tstamp',
    dist='event_id',
    tags=["derived"],
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "derived_tstamp",
      "data_type": "timestamp"
    }, databricks_val = 'derived_tstamp_date'),
    cluster_by=snowplow_web.web_cluster_by_fields_consent(),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    },
    snowplow_optimize= true
  )
}}

select
  *
  {% if target.type in ['databricks', 'spark'] -%}
  , DATE(derived_tstamp) as derived_tstamp_date
  {%- endif %}

from {{ ref('snowplow_web_consent_events_this_run') }}

where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
