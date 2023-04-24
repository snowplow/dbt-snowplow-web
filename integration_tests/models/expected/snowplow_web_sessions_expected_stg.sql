
SELECT
    APP_ID,domain_sessionid
    ,domain_sessionidx
    ,start_tstamp
    ,end_tstamp
    ,user_id
    ,domain_userid
    ,stitched_user_id
    ,network_userid
    ,page_views
    ,engaged_time_in_s
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
    ,geo_country
    ,geo_region
    ,geo_region_name
    ,geo_city
    ,geo_zipcode
    ,geo_latitude
    ,geo_longitude
    ,geo_timezone
    ,user_ipaddress
    ,useragent
    ,br_renderengine
    ,br_lang
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
{% elif target.type in ['bigquery'] %}
    ,cv_view_page_volume
    {# BQ can't compare array columns #}
    ,TO_JSON_STRING(ARRAY(SELECT replace(x, '"', '') FROM UNNEST(JSON_EXTRACT_ARRAY(cv_view_page_events,'$')) as x)) as cv_view_page_events
    ,TO_JSON_STRING(ARRAY(SELECT CAST(x AS FLOAT64) FROM UNNEST(JSON_EXTRACT_ARRAY(cv_view_page_values,'$')) as x)) as cv_view_page_values
    ,cv_view_page_total
    ,cv_view_page_first_conversion
    ,cv_view_page_converted
{% elif target.type in ['spark', 'databricks'] %}
    ,cv_view_page_volume
    {# thank you chatGPT #}
    ,filter(transform(split(regexp_replace(substring(cv_view_page_events, 3, length(cv_view_page_events)-3), '\\"+', ''), ','), x -> CAST(trim(x) AS string)), x -> x is not null and x != '')  as cv_view_page_events
    ,filter(transform(split(regexp_replace(substring(cv_view_page_values, 3, length(cv_view_page_values)-3), '\\"+', ''), ','), x -> CAST(trim(x) AS double)), x -> x is not null)  as cv_view_page_values
    ,cv_view_page_total
    ,cv_view_page_first_conversion
    ,cv_view_page_converted
{% elif target.type in ['postgres'] %}
    ,cv_view_page_volume
    ,string_to_array(regexp_replace(cv_view_page_events, '[\[\]\"]', '', 'g'),',') as cv_view_page_events
    ,string_to_array(regexp_replace(cv_view_page_values, '[\[\]\"]', '', 'g'),',')::numeric[] as cv_view_page_values
    ,cv_view_page_total
    ,cv_view_page_first_conversion
    ,cv_view_page_converted
{% endif %}



FROM {{ ref('snowplow_web_sessions_expected') }}
