{{ 
  config(
    sort='domain_userid',
    dist='domain_userid'
  ) 
}}

select
  a.domain_userid,
  b.start_tstamp

from {{ ref('snowplow_web_sessions_users_this_run') }}  a
inner join {{ ref('snowplow_web_users_manifest') }} b
on a.domain_userid = b.domain_userid
