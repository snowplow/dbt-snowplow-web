{# Default: Allow refresh in dev, block refresh otherwise. dev defined by snowplow__dev_target_name #}

{% macro allow_refresh() %}
  {{ return(adapter.dispatch('allow_refresh', 'snowplow_web')()) }}
{% endmacro %}

{% macro default__allow_refresh() %}
  
  {% set allow_refresh = snowplow_utils.get_value_by_target(
                                    dev_value=none,
                                    default_value=var('snowplow__allow_refresh'),
                                    dev_target_name=var('snowplow__dev_target_name')
                                    ) %}

  {{ return(allow_refresh) }}

{% endmacro %}
