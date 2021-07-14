{% macro get_iab_fields(enabled) %}

  {%- set iab_fields = ['category', 'primary_impact', 'reason', 'spider_or_robot'] -%}

  {%- if enabled -%}
    {%- set combined_iab_fields = snowplow_utils.combine_column_versions(
                                    relation=ref('snowplow_web_base_events_this_run'),
                                    column_prefix='contexts_com_iab_snowplow_spiders_and_robots_1',
                                    source_fields=iab_fields,
                                    relation_alias='ev'
                                    ) -%}

    {{ combined_iab_fields|join(',\n') }}
  {%- else -%}
    cast(null as {{ dbt_utils.type_string() }}) as category,
    cast(null as {{ dbt_utils.type_string() }}) as primary_impact,
    cast(null as {{ dbt_utils.type_string() }}) as reason,
    cast(null as boolean) as spider_or_robot
  {%- endif -%}

{% endmacro %}

{% macro get_ua_fields(enabled) %}

  {%- set ua_fields = ['useragent_family', 'useragent_major', 'useragent_minor', 'useragent_patch', 'useragent_version', 
                       'os_family', 'os_major', 'os_minor', 'os_patch', 'os_patch_minor', 'os_version', 'device_family'] -%}

  {%- if enabled -%}

    {%- set combined_ua_fields = snowplow_utils.combine_column_versions(
                                    relation=ref('snowplow_web_base_events_this_run'),
                                    column_prefix='contexts_com_snowplowanalytics_snowplow_ua_parser_context_1',
                                    source_fields=ua_fields,
                                    relation_alias='ev'
                                    ) -%}

    {{ combined_ua_fields|join(',\n') }}

  {%- else -%}

    {% for field in ua_fields %}
      cast(null as {{ dbt_utils.type_string() }}) as {{ field }} {% if not loop.last %}, {% endif %}
    {% endfor %}

  {%- endif -%}

{% endmacro %}

{% macro get_yauaa_fields(enabled) %}

  {%- set yauaa_fields = ['device_class', 'agent_class', 'agent_name', 'agent_name_version', 'agent_name_version_major',
                          'agent_version', 'agent_version_major', 'device_brand', 'device_name', 'device_version',
                          'layout_engine_class', 'layout_engine_name', 'layout_engine_name_version', 'layout_engine_name_version_major',
                          'layout_engine_version', 'layout_engine_version_major', 'operating_system_class', 'operating_system_name',
                          'operating_system_name_version', 'operating_system_version' ] -%}

  {%- if enabled -%}

    {%- set combined_yauaa_fields = snowplow_utils.combine_column_versions(
                                      relation=ref('snowplow_web_base_events_this_run'),
                                      column_prefix='contexts_nl_basjes_yauaa_context_1',
                                      source_fields=yauaa_fields,
                                      relation_alias='ev'
                                      ) -%}
    
    {{ combined_yauaa_fields|join(',\n') }}

  {%- else -%}

    {% for field in yauaa_fields %}
      cast(null as {{ dbt_utils.type_string() }}) as {{ field }} {% if not loop.last %}, {% endif %}
    {% endfor %}
    
  {%- endif -%}

{% endmacro %}
