{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

-- Removing model_tstamp

select
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
    {%- if var('snowplow__list_event_counts', false) %}
    -- just compare the string version for simplicity...
    {% if target.type == 'bigquery' %}
    ,to_json_string(event_counts) as event_counts
    {% elif target.type =='redshift' %}
    ,json_serialize(event_counts) as event_counts
    {% else %}
    ,cast(event_counts as {{snowplow_utils.type_max_string() }}) as event_counts
    {% endif %}
    {%- endif %}
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
{% if var('snowplow__conversion_events', none) %}
    ,  cv_view_page_volume
    {% if target.type == 'bigquery' %}
    ,  to_json_string(cv_view_page_events) as cv_view_page_events
    ,  to_json_string(cv_view_page_values) as cv_view_page_values
    {% else %}
    ,  cv_view_page_events
    ,  cv_view_page_values
    {% endif %}
    ,  cv_view_page_total
    ,  cv_view_page_first_conversion
    ,  cv_view_page_converted
    {% if var('snowplow__total_all_conversions') %}
    ,  cv__all_volume
    ,  cv__all_total
    {% endif %}
{% endif %}
    ,event_id
    ,event_id2


from {{ ref('snowplow_unified_sessions') }}
