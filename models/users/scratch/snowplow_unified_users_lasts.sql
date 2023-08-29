{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

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
  a.last_page_urlfragment,

  a.last_geo_country,
  a.last_geo_country_name,
  a.last_geo_continent,
  a.last_geo_city,
  a.last_geo_region_name,
  a.last_br_lang,
  a.last_br_lang_name

  {%- if var('snowplow__user_last_passthroughs', []) -%}
    {%- for identifier in var('snowplow__user_last_passthroughs', []) %}
    {# Check if it's a simple column or a sql+alias #}
    {%- if identifier is mapping -%}
        ,{{identifier['sql']}} as {{identifier['alias']}}
    {%- else -%}
        ,a.{{identifier}} as last_{{identifier}}
    {%- endif -%}
    {% endfor -%}
  {%- endif %}

from {{ ref('snowplow_unified_users_sessions_this_run') }} a

inner join {{ ref('snowplow_unified_users_aggs') }} b
on a.domain_sessionid = b.last_domain_sessionid
