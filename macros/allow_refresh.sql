{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{# Default: Allow refresh in dev, block refresh otherwise. dev defined by snowplow__dev_target_name #}

{% macro allow_refresh() %}
  {{ return(adapter.dispatch('allow_refresh', 'snowplow_web')()) }}
{% endmacro %}

{% macro default__allow_refresh() %}

  {% if flags.FULL_REFRESH == True %}
    {% set allow_refresh = snowplow_utils.get_value_by_target(
                                      dev_value=none,
                                      default_value=var('snowplow__allow_refresh'),
                                      dev_target_name=var('snowplow__dev_target_name')
                                      ) %}
  {% else %}
    {% set allow_refresh = none %}
  {% endif %}

  {{ return(allow_refresh) }}

{% endmacro %}
