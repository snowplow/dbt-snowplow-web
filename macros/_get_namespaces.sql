{% macro _get_namespaces() %}
  {% set override_namespaces = var('snowplow_web_dispatch_list', []) %}
  {% do return(override_namespaces + ['snowplow_web']) %}
{% endmacro %}
