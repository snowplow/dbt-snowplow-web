{# Test out the overwrite works by taking a false to a true #}

{% macro default__engaged_session() %}
    case when a.domain_sessionid = '0b0c7bb589ebd041177514f3e43446ca5d4343328936d2f8f12a42b41bf9140e' then true
    else
    page_views >= 2 or engaged_time_in_s / {{ var('snowplow__heartbeat', 10) }} >= 2
    {%- if var('snowplow__conversion_events', none) %}
        {%- for conv_def in var('snowplow__conversion_events') %}
            or cv_{{ conv_def['name'] }}_converted
        {%- endfor %}
    {%- endif %} end
{% endmacro %}
