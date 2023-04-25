{% macro core_web_vital_pass_query() %}
  {{ return(adapter.dispatch('core_web_vital_pass_query', 'snowplow_web')()) }}
{%- endmacro -%}

{% macro default__core_web_vital_pass_query() %}

case when m.lcp_result = 'good' and m.fid_result = 'good' and m.cls_result = 'good' then 1 else 0 end

{% endmacro %}
