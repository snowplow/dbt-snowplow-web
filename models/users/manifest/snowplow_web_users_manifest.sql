{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='domain_userid',
    upsert_date_key='start_tstamp',
    disable_upsert_lookback=true,
    full_refresh=false,
    schema=var("snowplow__manifest_custom_schema"),
    sort='start_tstamp',
    dist='domain_userid'
  ) 
}}

-- TODO: Consider retiring this model by adapting sessions_lifecycle_manifest to include `domain_userid`.
-- Think through edge case of multiple `domain_userid` per session

{% if snowplow_utils.snowplow_is_incremental() %}

  select
    utr.domain_userid,
    least(utr.min_tstamp, self.start_tstamp) as start_tstamp


  from {{ ref('snowplow_web_sessions_users_this_run') }} utr
  left join {{this}} self 
  on utr.domain_userid = self.domain_userid

  where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --don't reprocess users that have already been processed.

{% else %}

  select
    utr.domain_userid,
    utr.min_tstamp as start_tstamp

  from {{ ref('snowplow_web_sessions_users_this_run') }} utr

{% endif %}
