{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

-- `this_run` table so calc in drop and recompute fashion. This will be joined into the `snowplow_web_sessions_custom` incremental table 
{{ 
  config(
    sort='domain_sessionid',
    dist='domain_sessionid',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  ) 
}}

select 
  domain_sessionid,
  cast(sum(case when page_urlpath like 'https://www.mysite.com/products%' then 1 else 0 end) as boolean) as is_session_w_intent,
  cast(sum(case when page_urlpath like 'https://www.mysite.com/order_complete%' then 1 else 0 end) as boolean) as is_session_w_conversion

from {{ ref('snowplow_web_page_views_this_run') }} pv
group by 1
