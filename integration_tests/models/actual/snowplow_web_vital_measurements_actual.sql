{{
  config(
    enabled=var("snowplow__enable_cwv", false) and target.type in ('databricks', 'spark', 'snowflake', 'bigquery') | as_bool()
    )
}}

select *
{% if target.type == 'bigquery' %}
    except(time_period),
    replace(time_period, '+00', '') as time_period
{% endif %}
from {{ ref('snowplow_web_vital_measurements') }}
