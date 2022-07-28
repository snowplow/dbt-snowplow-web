{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='user_primary_key',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='user_primary_key'
  ) 
}}


select 
  CONCAT(u.domain_userid, "-"  s.user_ipaddress) as user_primary_key,
  u.*,


from {{ ref('snowplow_web_users_this_run') }} u -- join sessions_this_run to sessions_conversion_this_run to produce complete sessions table
left join {{ ref('snowplow_web_sessions')}} s on u.domain_userid = s.domain_userid
where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
