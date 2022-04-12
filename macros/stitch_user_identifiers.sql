{% macro stitch_user_identifiers(enabled, relation=this, user_mapping_relation=ref('snowplow_web_user_mapping')) %}  
  
  {% if enabled %}
  
    -- Update sessions table with mapping
    update {{ relation }} as s
    set stitched_user_id = um.user_id
    from {{ user_mapping_relation }} as um
    where s.domain_userid = um.domain_userid;

  {% endif %}

{% endmacro %}
