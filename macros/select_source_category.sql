{% macro select_source_category(source_category) %}
   select source from {{ ref('ga4_source_categories') }} where source_category = {{source_category}}
{% endmacro %}