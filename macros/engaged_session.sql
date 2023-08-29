{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro engaged_session() %}
    {{ return(adapter.dispatch('engaged_session', 'snowplow_unified')()) }}
{% endmacro %}

{% macro default__engaged_session() %}
    page_views >= 2 or engaged_time_in_s / {{ var('snowplow__heartbeat', 10) }} >= 2
    {%- if var('snowplow__conversion_events', none) %}
        {%- for conv_def in var('snowplow__conversion_events') %}
            or cv_{{ conv_def['name'] }}_converted
        {%- endfor %}
    {%- endif %}
{% endmacro %}
