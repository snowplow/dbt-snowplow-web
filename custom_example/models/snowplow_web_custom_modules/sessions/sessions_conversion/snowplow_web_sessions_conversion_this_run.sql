-- `this_run` table so calc in drop and recompute fashion. This will be joined into the `snowplow_web_sessions_custom` incremental table 
{{ 
  config(
    materialized='table', 
    sort='domain_sessionid',
    dist='domain_sessionid'
  ) 
}}

select 
  domain_sessionid,
  cast(sum(case when page_urlpath like 'https://www.mysite.com/products%' then 1 else 0 end) as boolean) as is_session_w_intent,
  cast(sum(case when page_urlpath like 'https://www.mysite.com/order_complete%' then 1 else 0 end) as boolean) as is_session_w_conversion

from {{ ref('snowplow_web_page_views_this_run') }} pv
group by 1
