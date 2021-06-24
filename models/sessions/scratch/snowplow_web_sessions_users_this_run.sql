{{ 
  config(
    sort='domain_userid',
    dist='domain_userid',
    tags=["this_run"]
  ) 
}}

select
  domain_userid,
  min(start_tstamp) as min_tstamp

from {{ ref('snowplow_web_sessions_this_run') }}
where domain_userid is not null
group by 1
