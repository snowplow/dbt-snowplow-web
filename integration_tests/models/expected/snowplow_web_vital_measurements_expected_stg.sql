{{
  config(
    enabled=var("snowplow__enable_cwv", false) and target.type in ('databricks', 'spark', 'snowflake', 'bigquery') | as_bool()
    )
}}

select *

from {{ ref('snowplow_web_vital_measurements_expected') }}
