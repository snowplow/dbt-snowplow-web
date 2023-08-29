{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select
  -- user fields
  a.user_id,
  a.domain_userid,
  a.original_domain_userid,
  a.network_userid,

  b.start_tstamp,
  b.end_tstamp,
  {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp,

  -- engagement fields
  b.page_views,
  b.sessions,

  b.engaged_time_in_s,

  -- first page fields
  a.first_page_title,
  a.first_page_url,
  a.first_page_urlscheme,
  a.first_page_urlhost,
  a.first_page_urlpath,
  a.first_page_urlquery,
  a.first_page_urlfragment,

  a.geo_country as first_geo_country,
  a.geo_country_name as first_geo_country_name,
  a.geo_continent as first_geo_continent,
  a.geo_city as first_geo_city,
  a.geo_region_name as first_geo_region_name,
  a.br_lang as first_br_lang,
  a.br_lang_name as first_br_lang_name,

  c.last_page_title,
  c.last_page_url,
  c.last_page_urlscheme,
  c.last_page_urlhost,
  c.last_page_urlpath,
  c.last_page_urlquery,
  c.last_page_urlfragment,

  c.last_geo_country,
  c.last_geo_country_name,
  c.last_geo_continent,
  c.last_geo_city,
  c.last_geo_region_name,
  c.last_br_lang,
  c.last_br_lang_name,


  -- referrer fields
  a.referrer,

  a.refr_urlscheme,
  a.refr_urlhost,
  a.refr_urlpath,
  a.refr_urlquery,
  a.refr_urlfragment,

  a.refr_medium,
  a.refr_source,
  a.refr_term,

  -- marketing fields
  a.mkt_medium,
  a.mkt_source,
  a.mkt_term,
  a.mkt_content,
  a.mkt_campaign,
  a.mkt_clickid,
  a.mkt_network,
  a.mkt_source_platform,
  a.default_channel_group

  {%- if var('snowplow__user_first_passthroughs', []) -%}
    {%- for identifier in var('snowplow__user_first_passthroughs', []) %}
    {# Check if it's a simple column or a sql+alias #}
    {%- if identifier is mapping -%}
        ,{{identifier['sql']}} as {{identifier['alias']}}
    {%- else -%}
        ,a.{{identifier}} as first_{{identifier}}
    {%- endif -%}
    {% endfor -%}
  {%- endif %}
  {%- if var('snowplow__user_last_passthroughs', []) -%}
    {%- for identifier in var('snowplow__user_last_passthroughs', []) %}
    {# Check if it's a simple column or a sql+alias #}
    {%- if identifier is mapping -%}
        ,c.{{identifier['alias']}}
    {%- else -%}
        ,c.last_{{identifier}}
    {%- endif -%}
    {% endfor -%}
  {%- endif %}

from {{ ref('snowplow_unified_users_aggs') }} as b

inner join {{ ref('snowplow_unified_users_sessions_this_run') }} as a
on a.domain_sessionid = b.first_domain_sessionid

inner join {{ ref('snowplow_unified_users_lasts') }} c
on b.domain_userid = c.domain_userid
