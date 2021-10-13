{# Destructive macro. Use with care! #}

{% macro post_ci_cleanup(schema_pattern=target.schema~'%') %}
  
  {# Get all schemas with the target.schema prefix #}
  {% set get_tables_sql = dbt_utils.get_tables_by_pattern_sql(schema_pattern,table_pattern='%') %}
  {% set results = run_query(get_tables_sql) %}
  {% set schemas = results|map(attribute='table_schema')|unique|list %}

  {% if schemas|length %}

    {# Generate sql to drop all identified schemas #}
    {% set drop_schema_sql -%}

      {% for schema in schemas -%}
        DROP SCHEMA IF EXISTS {{schema}} CASCADE; 
      {% endfor %}

    {%- endset %}

    {# Drop schemas #}
    {% do run_query(drop_schema_sql) %}

  {% endif %}

{% endmacro %}
