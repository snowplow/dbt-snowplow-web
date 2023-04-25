{{
  config(
    enabled=var("snowplow__enable_consent", false)
    )
}}

select *

from {{ ref('snowplow_web_consent_totals') }}
