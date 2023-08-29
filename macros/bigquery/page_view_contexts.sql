{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro iab_fields() %}
  
  {% set iab_fields = [
      {'field':'category', 'dtype':'string'},
      {'field':'primary_impact', 'dtype':'string'},
      {'field':'reason', 'dtype':'string'},
      {'field':'spider_or_robot', 'dtype':'boolean'}
    ] %}

  {{ return(iab_fields) }}

{% endmacro %}

{% macro ua_fields() %}
  
  {% set ua_fields = [
      {'field': 'useragent_family', 'dtype': 'string'},
      {'field': 'useragent_major', 'dtype': 'string'},
      {'field': 'useragent_minor', 'dtype': 'string'},
      {'field': 'useragent_patch', 'dtype': 'string'},
      {'field': 'useragent_version', 'dtype': 'string'},
      {'field': 'os_family', 'dtype': 'string'},
      {'field': 'os_major', 'dtype': 'string'},
      {'field': 'os_minor', 'dtype': 'string'},
      {'field': 'os_patch', 'dtype': 'string'},
      {'field': 'os_patch_minor', 'dtype': 'string'},
      {'field': 'os_version', 'dtype': 'string'},
      {'field': 'device_family', 'dtype': 'string'}
    ] %}

  {{ return(ua_fields) }}

{% endmacro %}

{% macro yauaa_fields() %}
  
  {% set yauaa_fields = [
      {'field': 'device_class', 'dtype': 'string'},
      {'field': 'agent_class', 'dtype': 'string'},
      {'field': 'agent_name', 'dtype': 'string'},
      {'field': 'agent_name_version', 'dtype': 'string'},
      {'field': 'agent_name_version_major', 'dtype': 'string'},
      {'field': 'agent_version', 'dtype': 'string'},
      {'field': 'agent_version_major', 'dtype': 'string'},
      {'field': 'device_brand', 'dtype': 'string'},
      {'field': 'device_name', 'dtype': 'string'},
      {'field': 'device_version', 'dtype': 'string'},
      {'field': 'layout_engine_class', 'dtype': 'string'},
      {'field': 'layout_engine_name', 'dtype': 'string'},
      {'field': 'layout_engine_name_version', 'dtype': 'string'},
      {'field': 'layout_engine_name_version_major', 'dtype': 'string'},
      {'field': 'layout_engine_version', 'dtype': 'string'},
      {'field': 'layout_engine_version_major', 'dtype': 'string'},
      {'field': 'operating_system_class', 'dtype': 'string'},
      {'field': 'operating_system_name', 'dtype': 'string'},
      {'field': 'operating_system_name_version', 'dtype': 'string'},
      {'field': 'operating_system_version', 'dtype': 'string'}
    ] %}

  {{ return(yauaa_fields) }}

{% endmacro %}
