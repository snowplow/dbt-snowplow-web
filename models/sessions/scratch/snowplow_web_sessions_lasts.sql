{{ 
  config(
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["domain_sessionid"]),
    sort='domain_sessionid',
    dist='domain_sessionid'
  ) 
}}

select
  a.domain_sessionid,
  a.page_title as last_page_title,

  a.page_url as last_page_url,

  a.page_urlscheme as last_page_urlscheme,
  a.page_urlhost as last_page_urlhost,
  a.page_urlpath as last_page_urlpath,
  a.page_urlquery as last_page_urlquery,
  a.page_urlfragment as last_page_urlfragment

from {{ ref('snowplow_web_page_views_this_run') }} a

inner join {{ ref('snowplow_web_sessions_aggs') }} b
on a.domain_sessionid = b.domain_sessionid
-- don't join on timestamp because people can return to a page after previous page view is complete.
and a.page_view_in_session_index = b.page_views

