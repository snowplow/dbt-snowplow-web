{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

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
