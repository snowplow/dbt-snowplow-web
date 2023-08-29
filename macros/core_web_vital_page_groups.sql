{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro core_web_vital_page_groups() %}
  {{ return(adapter.dispatch('core_web_vital_page_groups', 'snowplow_unified')()) }}
{%- endmacro -%}

{% macro default__core_web_vital_page_groups() %}

  case when page_url like '%/product%' then 'PDP'
      when page_url like '%/list%' then 'PLP'
      when page_url like '%/checkout%' then 'checkout'
      when page_url like '%/home%' then 'homepage'
      else 'other' end

{% endmacro %}
