{% macro stitch_user_identifiers(enabled, relation=this, user_mapping_relation='snowplow_web_user_mapping') %}
    {{ return(adapter.dispatch('stitch_user_identifiers', 'snowplow_web')(enabled, relation, user_mapping_relation)) }}
{%- endmacro -%}

{% macro default__stitch_user_identifiers(enabled, relation=this, user_mapping_relation='snowplow_web_user_mapping') %}
    {% if enabled | as_bool() %}
      -- Update sessions table with mapping
      update {{ relation }} as s
      set stitched_user_id = um.user_id
      from {{ ref(user_mapping_relation) }} as um
      where s.domain_userid = um.domain_userid;
    {% endif %}
{%- endmacro -%}

{% macro spark__stitch_user_identifiers(enabled, relation=this, user_mapping_relation='snowplow_web_user_mapping') %}
    {% if enabled | as_bool() %}
      -- Update sessions table with mapping
      merge into {{ relation }} as s
      using {{ ref(user_mapping_relation) }} as um
      on s.domain_userid = um.domain_userid
      when matched then
      update set s.stitched_user_id = um.user_id;
    {% endif %}
{%- endmacro -%}
