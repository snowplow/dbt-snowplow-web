{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
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
on a.domain_sessionid = b.last_domain_sessionid
