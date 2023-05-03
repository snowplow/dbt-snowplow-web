{% macro engaged_session() %}
    {{ return(adapter.dispatch('engaged_session', 'snowplow_web')()) }}
{% endmacro %}

{% macro default__engaged_session() %}
    page_views >= 2 or engaged_time_in_s / {{ var('snowplow__heartbeat', 10) }} >= 2
    {%- if var('snowplow__conversion_events', none) %}
        {%- for conv_def in var('snowplow__conversion_events') %}
            or cv_{{ conv_def['name'] }}_converted
        {%- endfor %}
    {%- endif %}
{% endmacro %}
