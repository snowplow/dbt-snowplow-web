{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro config_check() %}
  {{ return(adapter.dispatch('config_check', 'snowplow_unified')()) }}
{% endmacro %}

{% macro default__config_check() %}

  {% if not var('snowplow__enable_web') and not var('snowplow__enable_mobile') %}
    {{ exceptions.raise_compiler_error(
      "Snowplow Error: No platform to process. Please set at least one of the variables `snowplow__enable_web` or `snowplow__enable_web` to true."
    ) }}
  {% endif %}

  {% if not var('snowplow__enable_web') %}
    {% if var('snowplow__enable_iab') or var('snowplow__enable_ua') or var('snowplow__enable_yauaa') %}
      {% do exceptions.warn("Snowplow Warning: Please note that you have web contexts enabled but those won't be processed as var('snowplow__enable_web') is currently disabled.") %}
    {% endif %}
  {% endif %}

  {% if not var('snowplow__enable_mobile') %}
    {% if var('snowplow__enable_mobile_context') or var('snowplow__enable_geolocation_context') or var('snowplow__enable_application_context') or var('snowplow__enable_screen_context') or var('snowplow__enable_app_errors_module') %}
      {% do exceptions.warn("Snowplow Warning: Please note that you have mobile contexts enabled but those won't be processed as var('snowplow__enable_mobile') is currently disabled.") %}
    {% endif %}
  {% endif %}

{% endmacro %}
