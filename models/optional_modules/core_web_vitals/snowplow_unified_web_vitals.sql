{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    materialized= 'incremental',
    enabled=var("snowplow__enable_cwv", false) | as_bool(),
    unique_key='page_view_id',
    upsert_date_key='derived_tstamp',
    sort='derived_tstamp',
    dist='page_view_id',
    tags=["derived"],
    partition_by = snowplow_utils.get_value_by_target_type(bigquery_val = {
      "field": "derived_tstamp",
      "data_type": "timestamp"
    }, databricks_val = 'derived_tstamp_date'),
    cluster_by=snowplow_unified.web_cluster_by_fields_cwv(),
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

from {{ ref('snowplow_unified_web_vitals_this_run') }}

where {{ snowplow_utils.is_run_with_new_events('snowplow_unified') }} --returns false if run doesn't contain new events.
