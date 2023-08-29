{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro get_conversion_columns(conv_object = {}, names_only = false) %}
  {{ return(adapter.dispatch('get_conversion_columns', 'snowplow_unified')(conv_object, names_only)) }}
{% endmacro %}

{% macro default__get_conversion_columns(conv_object, names_only = false) %}
{% if execute %}
    {% do exceptions.raise_compiler_error('Macro get_field only supports Bigquery, Snowflake, Spark, Databricks, Postgres, and Redshift, it is not supported for ' ~ target.type) %}
{% endif %}
{% endmacro %}

{% macro snowflake__get_conversion_columns(conv_object, names_only = false) %}
  {%- if not names_only %}
    ,COUNT(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE null END) AS cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    ,ARRAYAGG(CASE WHEN {{ conv_object['condition'] }} THEN event_id ELSE null END) WITHIN GROUP (ORDER BY derived_tstamp, dvce_created_tstamp, event_id) AS cv_{{ conv_object['name'] }}_events
    {%- endif -%}
    {%- if conv_object.get('value', none) %}
    ,ARRAYAGG(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }},{{ conv_object.get('default_value', 0) }})  ELSE null END) WITHIN GROUP (ORDER BY derived_tstamp, dvce_created_tstamp, event_id) AS cv_{{ conv_object['name'] }}_values
    ,SUM(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }}, {{ conv_object.get('default_value', 0) }}) ELSE 0 END) AS cv_{{ conv_object['name'] }}_total
    {%- endif %}
    ,MIN(CASE WHEN {{ conv_object['condition'] }} THEN derived_tstamp ELSE null END) AS cv_{{ conv_object['name'] }}_first_conversion
    ,CAST(MAX(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE 0 END) AS {{ type_boolean() }}) AS cv_{{ conv_object['name'] }}_converted
  {%- else -%}
    ,cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    ,cv_{{ conv_object['name'] }}_events
    {%- endif %}
    {%- if conv_object.get('value', none) %}
    ,cv_{{ conv_object['name'] }}_values
    ,cv_{{ conv_object['name'] }}_total
    {%- endif %}
    ,cv_{{ conv_object['name'] }}_first_conversion
    ,cv_{{ conv_object['name'] }}_converted
  {%- endif %}
{% endmacro %}


{% macro bigquery__get_conversion_columns(conv_object, names_only = false) %}
  {%- if not names_only %}
    ,COUNT(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE null END) AS cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    ,ARRAY_AGG(CASE WHEN {{ conv_object['condition'] }} THEN event_id ELSE null END IGNORE NULLS ORDER BY derived_tstamp, dvce_created_tstamp, event_id) AS cv_{{ conv_object['name'] }}_events
    {%- endif -%}
    {%- if conv_object.get('value', none) %}
    ,ARRAY_AGG(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }},{{ conv_object.get('default_value', 0) }})  ELSE null END IGNORE NULLS ORDER BY derived_tstamp, dvce_created_tstamp, event_id) AS cv_{{ conv_object['name'] }}_values
    ,SUM(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }}, {{ conv_object.get('default_value', 0) }})  ELSE 0 END) AS cv_{{ conv_object['name'] }}_total
    {%- endif -%}
    ,MIN(CASE WHEN {{ conv_object['condition'] }} THEN derived_tstamp ELSE null END) AS cv_{{ conv_object['name'] }}_first_conversion
    ,CAST(MAX(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE 0 END) AS {{ type_boolean() }}) AS cv_{{ conv_object['name'] }}_converted
  {%- else -%}
    ,cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    ,cv_{{ conv_object['name'] }}_events
    {%- endif %}
    {%- if conv_object.get('value', none) %}
    ,cv_{{ conv_object['name'] }}_values
    ,cv_{{ conv_object['name'] }}_total
    {%- endif %}
    ,cv_{{ conv_object['name'] }}_first_conversion
    ,cv_{{ conv_object['name'] }}_converted
  {%- endif %}
{% endmacro %}


{% macro spark__get_conversion_columns(conv_object, names_only = false) %}
  {%- if not names_only %}
    ,COUNT(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE null END) AS cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    {# make an struct of the thing we want to put in an array, then the things we want to order by, collect THOSE into an array, filter out where the thing we want is null, sort those based on the other columns, then select just the thing we care about #}
    ,transform(array_sort(FILTER(collect_list(struct(CASE WHEN {{ conv_object['condition'] }} THEN event_id ELSE null END, derived_tstamp, dvce_created_tstamp, event_id)), x -> x['col1'] is not null), (left, right) -> CASE WHEN left['derived_tstamp']  < right['derived_tstamp'] THEN -1 WHEN left['derived_tstamp']  > right['derived_tstamp'] THEN 1 WHEN left['dvce_created_tstamp']  < right['dvce_created_tstamp'] THEN -1 WHEN left['dvce_created_tstamp']  > right['dvce_created_tstamp'] THEN 1 WHEN left['event_id']  < right['event_id'] THEN -1 WHEN left['event_id']  > right['event_id'] THEN 1 ELSE 0 END), x -> x['col1'])  AS cv_{{ conv_object['name'] }}_events
    {%- endif -%}
    {%- if conv_object.get('value', none) %}
    {# make an struct of the thing we want to put in an array, then the things we want to order by, collect THOSE into an array, filter out where the thing we want is null, sort those based on the other columns, then select just the thing we care about #}
    ,transform(array_sort(FILTER(collect_list(struct(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }},{{ conv_object.get('default_value', 0) }})  ELSE null END, derived_tstamp, dvce_created_tstamp, event_id)), x -> x['col1'] is not null), (left, right) -> CASE WHEN left['derived_tstamp']  < right['derived_tstamp'] THEN -1 WHEN left['derived_tstamp']  > right['derived_tstamp'] THEN 1 WHEN left['dvce_created_tstamp']  < right['dvce_created_tstamp'] THEN -1 WHEN left['dvce_created_tstamp']  > right['dvce_created_tstamp'] THEN 1 WHEN left['event_id']  < right['event_id'] THEN -1 WHEN left['event_id']  > right['event_id'] THEN 1 ELSE 0 END), x -> x['col1'])  AS cv_{{ conv_object['name'] }}_values
    ,SUM(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }}, {{ conv_object.get('default_value', 0) }})  ELSE 0 END) AS cv_{{ conv_object['name'] }}_total
    {%- endif -%}
    ,MIN(CASE WHEN {{ conv_object['condition'] }} THEN derived_tstamp ELSE null END) AS cv_{{ conv_object['name'] }}_first_conversion
    ,CAST(MAX(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE 0 END) AS {{ type_boolean() }}) AS cv_{{ conv_object['name'] }}_converted
  {%- else -%}
    ,cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    ,cv_{{ conv_object['name'] }}_events
    {%- endif %}
    {%- if conv_object.get('value', none) %}
    ,cv_{{ conv_object['name'] }}_values
    ,cv_{{ conv_object['name'] }}_total
    {%- endif %}
    ,cv_{{ conv_object['name'] }}_first_conversion
    ,cv_{{ conv_object['name'] }}_converted
  {%- endif %}
{% endmacro %}

{% macro postgres__get_conversion_columns(conv_object = {}, names_only = false) %}
  {%- if not names_only %}
    ,COUNT(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE null END) AS cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    ,ARRAY_REMOVE(ARRAY_AGG(CASE WHEN {{ conv_object['condition'] }} THEN event_id ELSE null END ORDER BY derived_tstamp, dvce_created_tstamp, event_id), null) AS cv_{{ conv_object['name'] }}_events
    {%- endif -%}
    {%- if conv_object.get('value', none) %}
    ,ARRAY_REMOVE(ARRAY_AGG(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }},{{ conv_object.get('default_value', 0) }})  ELSE null END ORDER BY derived_tstamp, dvce_created_tstamp, event_id), null) AS cv_{{ conv_object['name'] }}_values
    ,SUM(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }}, {{ conv_object.get('default_value', 0) }})  ELSE 0 END) AS cv_{{ conv_object['name'] }}_total
    {%- endif -%}
    ,MIN(CASE WHEN {{ conv_object['condition'] }} THEN derived_tstamp ELSE null END) AS cv_{{ conv_object['name'] }}_first_conversion
    ,CAST(MAX(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE 0 END) AS {{ type_boolean() }}) AS cv_{{ conv_object['name'] }}_converted
  {%- else -%}
    ,cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    ,cv_{{ conv_object['name'] }}_events
    {%- endif %}
    {%- if conv_object.get('value', none) %}
    ,cv_{{ conv_object['name'] }}_values
    ,cv_{{ conv_object['name'] }}_total
    {%- endif %}
    ,cv_{{ conv_object['name'] }}_first_conversion
    ,cv_{{ conv_object['name'] }}_converted
  {%- endif %}
{% endmacro %}

{% macro redshift__get_conversion_columns(conv_object, names_only = false) %}
  {%- if not names_only %}
    ,COUNT(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE null END) AS cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    ,SPLIT_TO_ARRAY(LISTAGG(CASE WHEN {{ conv_object['condition'] }} THEN event_id ELSE null END, ',') WITHIN GROUP (ORDER BY derived_tstamp, dvce_created_tstamp, event_id), ',') AS cv_{{ conv_object['name'] }}_events
    {%- endif -%}
    {%- if conv_object.get('value', none) %}
    {# Want to try and use a symbol that is unlikely to be in the values due to redshift not having a single array_agg function, hence ~ not , #}
    ,SPLIT_TO_ARRAY(LISTAGG(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }},{{ conv_object.get('default_value', 0) }})  ELSE null END, '~') WITHIN GROUP (ORDER BY derived_tstamp, dvce_created_tstamp, event_id), '~') AS cv_{{ conv_object['name'] }}_values
    ,SUM(CASE WHEN {{ conv_object['condition'] }} THEN coalesce({{ conv_object['value'] }}, {{ conv_object.get('default_value', 0) }})  ELSE 0 END) AS cv_{{ conv_object['name'] }}_total
    {%- endif -%}
    ,MIN(CASE WHEN {{ conv_object['condition'] }} THEN derived_tstamp ELSE null END) AS cv_{{ conv_object['name'] }}_first_conversion
    ,CAST(MAX(CASE WHEN {{ conv_object['condition'] }} THEN 1 ELSE 0 END) AS {{ type_boolean() }}) AS cv_{{ conv_object['name'] }}_converted
  {%- else -%}
    ,cv_{{ conv_object['name'] }}_volume
    {%- if conv_object.get('list_events', false) %}
    ,cv_{{ conv_object['name'] }}_events
    {%- endif %}
    {%- if conv_object.get('value', none) %}
    ,cv_{{ conv_object['name'] }}_values
    ,cv_{{ conv_object['name'] }}_total
    {%- endif %}
    ,cv_{{ conv_object['name'] }}_first_conversion
    ,cv_{{ conv_object['name'] }}_converted
  {%- endif %}
{% endmacro %}
