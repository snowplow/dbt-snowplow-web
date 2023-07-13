{{
  config(
    enabled=var("snowplow__enable_cwv", false) | as_bool()
    )
}}

select *

from {{ ref('snowplow_web_vital_measurements') }}
