{% macro core_web_vital_page_groups() %}
  {{ return(adapter.dispatch('core_web_vital_page_groups', 'snowplow_web')()) }}
{%- endmacro -%}

{% macro default__core_web_vital_page_groups() %}

  case when page_url like '%/product%' then 'PDP'
      when page_url like '%/list%' then 'PLP'
      when page_url like '%/checkout%' then 'checkout'
      when page_url like '%/home%' then 'homepage'
      else 'other' end

{% endmacro %}
