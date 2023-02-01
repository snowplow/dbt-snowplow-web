{% macro stitch_user_identifiers(enabled, relation=this, user_mapping_relation='snowplow_web_user_mapping') %}

  {% if enabled and target.type not in ['databricks', 'spark'] | as_bool() %}

    -- Update sessions table with mapping
    update {{ relation }} as s
    set stitched_user_id = um.user_id
    from {{ ref(user_mapping_relation) }} as um
    where s.domain_userid = um.domain_userid;

  {% elif enabled and target.type in ['databricks', 'spark']  | as_bool() %}

    -- Update sessions table with mapping
    merge into {{ relation }} as s
    using {{ ref(user_mapping_relation) }} as um
    on s.domain_userid = um.domain_userid
    when matched then
      update set s.stitched_user_id = um.user_id;

  {% endif %}

{% endmacro %}
