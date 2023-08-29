{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

-- Removing model_tstamp

SELECT
    app_id
    ,platform
    ,domain_sessionid
    ,original_domain_sessionid
    ,domain_sessionidx
    ,start_tstamp
    ,end_tstamp
    ,user_id
    ,domain_userid
    ,original_domain_userid
    ,stitched_user_id
    ,network_userid
    ,page_views
    ,engaged_time_in_s
    {% if target.type =='redshift' %}
    ,replace(event_counts, ' ', '') as event_counts
    {% else %}
    ,event_counts
    {% endif %}
    ,total_events
    ,is_engaged
    ,absolute_time_in_s
    ,first_page_title
    ,first_page_url
    ,first_page_urlscheme
    ,first_page_urlhost
    ,first_page_urlpath
    ,first_page_urlquery
    ,first_page_urlfragment
    ,last_page_title
    ,last_page_url
    ,last_page_urlscheme
    ,last_page_urlhost
    ,last_page_urlpath
    ,last_page_urlquery
    ,last_page_urlfragment
    ,referrer
    ,refr_urlscheme
    ,refr_urlhost
    ,refr_urlpath
    ,refr_urlquery
    ,refr_urlfragment
    ,refr_medium
    ,refr_source
    ,refr_term
    ,mkt_medium
    ,mkt_source
    ,mkt_term
    ,mkt_content
    ,mkt_campaign
    ,mkt_clickid
    ,mkt_network
    ,mkt_source_platform
    ,default_channel_group
    ,geo_country
    ,geo_region
    ,geo_region_name
    ,geo_city
    ,geo_zipcode
    ,geo_latitude
    ,geo_longitude
    ,geo_timezone
    ,geo_country_name
    ,geo_continent
    ,last_geo_country
    ,last_geo_region_name
    ,last_geo_city
    ,last_geo_country_name
    ,last_geo_continent
    ,user_ipaddress
    ,useragent
    ,br_renderengine
    ,br_lang
    ,br_lang_name
    ,last_br_lang
    ,last_br_lang_name
    ,os_timezone
    ,category
    ,primary_impact
    ,reason
    ,spider_or_robot
    ,useragent_family
    ,useragent_major
    ,useragent_minor
    ,useragent_patch
    ,useragent_version
    ,os_family
    ,os_major
    ,os_minor
    ,os_patch
    ,os_patch_minor
    ,os_version
    ,device_family
    ,device_class
    ,device_category
    ,screen_resolution
    ,agent_class
    ,agent_name
    ,agent_name_version
    ,agent_name_version_major
    ,agent_version
    ,agent_version_major
    ,device_brand
    ,device_name
    ,device_version
    ,layout_engine_class
    ,layout_engine_name
    ,layout_engine_name_version
    ,layout_engine_name_version_major
    ,layout_engine_version
    ,layout_engine_version_major
    ,operating_system_class
    ,operating_system_name
    ,operating_system_name_version
    ,operating_system_version
{% if target.type in ['snowflake'] %}
    ,cv_view_page_volume
    ,AS_ARRAY(parse_json(cv_view_page_events)) as cv_view_page_events
    ,AS_ARRAY(parse_json(cv_view_page_values)) as cv_view_page_values
    ,cv_view_page_total
    ,cv_view_page_first_conversion
    ,cv_view_page_converted
    ,cv__all_volume
    ,cv__all_total
{% elif target.type in ['bigquery'] %}
    ,cv_view_page_volume
    {# BQ can't compare array columns #}
    ,to_json_string(array(select replace(x, '"', '') from unnest(json_extract_array(cv_view_page_events,'$')) as x)) as cv_view_page_events
    ,to_json_string(array(select cast(x AS float64) from unnest(json_extract_array(cv_view_page_values,'$')) as x)) as cv_view_page_values
    ,cv_view_page_total
    ,cv_view_page_first_conversion
    ,cv_view_page_converted
    ,cv__all_volume
    ,cv__all_total
{% elif target.type in ['spark', 'databricks'] %}
    ,cv_view_page_volume
    {# thank you chatGPT #}
    ,filter(transform(split(regexp_replace(substring(cv_view_page_events, 3, length(cv_view_page_events)-3), '\\"+', ''), ','), x -> CAST(trim(x) AS string)), x -> x is not null and x != '')  as cv_view_page_events
    ,filter(transform(split(regexp_replace(substring(cv_view_page_values, 3, length(cv_view_page_values)-3), '\\"+', ''), ','), x -> CAST(trim(x) AS double)), x -> x is not null)  as cv_view_page_values
    ,cv_view_page_total
    ,cv_view_page_first_conversion
    ,cv_view_page_converted
    ,cv__all_volume
    ,cv__all_total
{% elif target.type in ['postgres', 'redshift'] %}
    ,cv_view_page_volume
    {% if target.type == 'redshift' %}
    ,nullif(split_to_array(translate(cv_view_page_events, '[]"]', ''),','), array()) as cv_view_page_events
    ,nullif(split_to_array(translate(cv_view_page_values, '[]"]', ''),','), array()) as cv_view_page_values
    {% else %}
    ,string_to_array(regexp_replace(cv_view_page_events, '[\[\]\"]', '', 'g'),',') as cv_view_page_events
    ,string_to_array(regexp_replace(cv_view_page_values, '[\[\]\"]', '', 'g'),',')::numeric[] as cv_view_page_values
    {% endif %}
    ,cv_view_page_total
    ,cv_view_page_first_conversion
    ,cv_view_page_converted
    ,cv__all_volume
    ,cv__all_total
{% endif %}
    ,event_id
    ,event_id2

FROM {{ ref('snowplow_unified_sessions_expected') }}
