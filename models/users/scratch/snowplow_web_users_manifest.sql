{{ 
  config(
    materialized='incremental',
    unique_key='domain_userid',
    sort='start_tstamp',
    dist='domain_userid'
  ) 
}}

-- Potentially can remove this model all together by adapting `snowplow_base_sessions_lifecycle`

{% if is_incremental() %}

  select
    utr.domain_userid,
    least(utr.min_tstamp, self.start_tstamp) as start_tstamp


  from {{ ref('snowplow_web_sessions_users_this_run') }} utr
  left join {{this}} self 
  on utr.domain_userid = self.domain_userid

  -- could add in where condition to stop reprocessing on backfills.

{% else %}

  select
    utr.domain_userid,
    utr.min_tstamp as start_tstamp

  from {{ ref('snowplow_web_sessions_users_this_run') }} utr

{% endif %}
