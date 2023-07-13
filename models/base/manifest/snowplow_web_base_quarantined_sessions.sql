{{
  config(
    materialized='incremental',
    full_refresh=snowplow_web.allow_refresh(),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt')),
    tblproperties={
      'delta.autoOptimize.optimizeWrite' : 'true',
      'delta.autoOptimize.autoCompact' : 'true'
    }
  )
}}

{% set quarantined_query = snowplow_utils.base_create_snowplow_quarantined_sessions() %}

{{ quarantined_query }}
