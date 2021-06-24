{{ 
  config(
    sort='domain_userid',
    dist='domain_userid'
  ) 
}}


select
  a.domain_userid,
  a.last_page_title,

  a.last_page_url,

  a.last_page_urlscheme,
  a.last_page_urlhost,
  a.last_page_urlpath,
  a.last_page_urlquery,
  a.last_page_urlfragment

from {{ ref('snowplow_web_users_sessions_this_run') }} a

inner join {{ ref('snowplow_web_users_aggs') }} b
on a.domain_userid = b.domain_userid
and a.end_tstamp = b.end_tstamp

