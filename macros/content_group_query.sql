{% macro content_group_query() %}
  {{ return(adapter.dispatch('content_group_query', 'snowplow_web')()) }}
{% endmacro %}


{% macro default__content_group_query() %}
  case when ev.page_url like '%/product%' then 'PDP'
      when ev.page_url like '%/list%' then 'PLP'
      when ev.page_url like '%/checkout%' then 'checkout'
      when ev.page_url like '%/home%' then 'homepage'
      else 'other'
  end

{% endmacro %}
