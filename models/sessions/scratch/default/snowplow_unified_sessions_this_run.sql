{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
    config(
        tags=["this_run"]
    )
}}

with session_firsts as (
    select
        -- app id
        app_id as app_id,

        platform,

        -- session fields
        domain_sessionid,
        original_domain_sessionid,
        domain_sessionidx,

        {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp,

        -- user fields
        user_id,
        domain_userid,
        original_domain_userid,
        {% if var('snowplow__session_stitching') %}
            -- updated with mapping as part of post hook on derived sessions table
            cast(domain_userid as {{ type_string() }}) as stitched_user_id,
        {% else %}
            cast(null as {{ snowplow_utils.type_max_string() }}) as stitched_user_id,
        {% endif %}
        network_userid as network_userid,

        -- first page fields
        page_title as first_page_title,
        page_url as first_page_url,
        page_urlscheme as first_page_urlscheme,
        page_urlhost as first_page_urlhost,
        page_urlpath as first_page_urlpath,
        page_urlquery as first_page_urlquery,
        page_urlfragment as first_page_urlfragment,

        -- referrer fields
        page_referrer as referrer,
        refr_urlscheme as refr_urlscheme,
        refr_urlhost as refr_urlhost,
        refr_urlpath as refr_urlpath,
        refr_urlquery as refr_urlquery,
        refr_urlfragment as refr_urlfragment,
        refr_medium as refr_medium,
        refr_source as refr_source,
        refr_term as refr_term,

        -- marketing fields
        mkt_medium as mkt_medium,
        mkt_source as mkt_source,
        mkt_term as mkt_term,
        mkt_content as mkt_content,
        mkt_campaign as mkt_campaign,
        mkt_clickid as mkt_clickid,
        mkt_network as mkt_network,
        {% if target.type in ['postgres'] %}
            (regexp_match(page_urlquery, 'utm_source_platform=([^?&#]*)'))[1] as mkt_source_platform,
        {% else %}
            nullif(regexp_substr(page_urlquery, 'utm_source_platform=([^?&#]*)', 1, 1, 'e'), '') as mkt_source_platform,
        {% endif %}
        {{ channel_group_query() }} as default_channel_group,

        -- geo fields
        geo_country as geo_country,
        geo_region as geo_region,
        geo_region_name as geo_region_name,
        geo_city as geo_city,
        geo_zipcode as geo_zipcode,
        geo_latitude as geo_latitude,
        geo_longitude as geo_longitude,
        geo_timezone as geo_timezone,
        g.name as geo_country_name,
        g.region as geo_continent,

        -- ip address
        user_ipaddress as user_ipaddress,

        -- user agent
        useragent as useragent,

        dvce_screenwidth || 'x' || dvce_screenheight as screen_resolution,

        br_renderengine as br_renderengine,
        br_lang as br_lang,
        l.name as br_lang_name,
        os_timezone as os_timezone,

        -- optional fields, only populated if enabled.
        -- iab enrichment fields: set iab variable to true to enable
        {{snowplow_unified.get_iab_context_fields('ev')}},

        -- ua parser enrichment fields
        {{snowplow_unified.get_ua_context_fields('ev')}},

        -- yauaa enrichment fields
        {{snowplow_unified.get_yauaa_context_fields('ev')}},

        row_number() over (partition by ev.domain_sessionid order by ev.derived_tstamp, ev.dvce_created_tstamp, ev.event_id) AS page_event_in_session_index,
        event_name
        {%- if var('snowplow__session_passthroughs', []) -%}
            {%- set passthrough_names = [] -%}
            {%- for identifier in var('snowplow__session_passthroughs', []) %}
            {# Check if it's a simple column or a sql+alias #}
            {%- if identifier is mapping -%}
                ,{{identifier['sql']}} as {{identifier['alias']}}
                {%- do passthrough_names.append(identifier['alias']) -%}
            {%- else -%}
                ,ev.{{identifier}}
                {%- do passthrough_names.append(identifier) -%}
            {%- endif -%}
            {% endfor -%}
        {%- endif %}
    from {{ ref('snowplow_unified_base_events_this_run') }}  ev
    left join
        {{ ref(var('snowplow__ga4_categories_seed')) }} c on lower(trim(ev.mkt_source)) = lower(c.source)
    left join
        {{ ref(var('snowplow__rfc_5646_seed')) }} l on lower(ev.br_lang) = lower(l.lang_tag)
    left join
        {{ ref(var('snowplow__geo_mapping_seed')) }} g on lower(ev.geo_country) = lower(g.alpha_2)

    where
        ev.event_name in ('page_ping', 'page_view')
        and ev.page_view_id is not null
        {% if var("snowplow__ua_bot_filter", true) %}
            {{ filter_bots('ev') }}
        {% endif %}
),

session_lasts as (
    select
        domain_sessionid,
        page_title as last_page_title,
        page_url as last_page_url,
        page_urlscheme as last_page_urlscheme,
        page_urlhost as last_page_urlhost,
        page_urlpath as last_page_urlpath,
        page_urlquery as last_page_urlquery,
        page_urlfragment as last_page_urlfragment,
        geo_country as last_geo_country,
        geo_city as last_geo_city,
        geo_region_name as last_geo_region_name,
        g.name as last_geo_country_name,
        g.region as last_geo_continent,
        br_lang as last_br_lang,
        l.name as last_br_lang_name,
        row_number() over (partition by ev.domain_sessionid order by ev.derived_tstamp desc, ev.dvce_created_tstamp desc, ev.event_id) AS page_event_in_session_index
    from {{ ref('snowplow_unified_base_events_this_run') }} ev
    left join
        {{ ref(var('snowplow__rfc_5646_seed')) }} l on lower(ev.br_lang) = lower(l.lang_tag)
    left join
        {{ ref(var('snowplow__geo_mapping_seed')) }} g on lower(ev.geo_country) = lower(g.alpha_2)
    where
        event_name in ('page_view')
        and page_view_id is not null
        {% if var("snowplow__ua_bot_filter", true) %}
            {{ filter_bots() }}
        {% endif %}
),

session_aggs as (
    select
        domain_sessionid
        , min(derived_tstamp) as start_tstamp
        , max(derived_tstamp) as end_tstamp
        {%- if var('snowplow__list_event_counts', false) %}
            {% set event_names =  dbt_utils.get_column_values(ref('snowplow_unified_base_events_this_run'), 'event_name', order_by = 'event_name') %}
            {# Loop over every event_name in this run, create a json string of the name and count ONLY if there are events with that name in the session (otherwise empty string),
                then trim off the last comma (can't use loop.first/last because first/last entry may not have any events for that session)
            #}
            , '{' || rtrim(
            {%- for event_name in event_names %}
                case when sum(case when event_name = '{{event_name}}' then 1 else 0 end) > 0 then '"{{event_name}}" :' || sum(case when event_name = '{{event_name}}' then 1 else 0 end) || ', ' else '' end ||
            {%- endfor -%}
            '', ', ') || '}' as event_counts_string
        {% endif %}
        , count(*) as total_events
        -- engagement fields
        , count(distinct case when event_name in ('page_ping', 'page_view') and page_view_id is not null then page_view_id else null end) as page_views
        -- (hb * (#page pings - # distinct page view ids ON page pings)) + (# distinct page view ids ON page pings * min visit length)
        , ({{ var("snowplow__heartbeat", 10) }} * (
                -- number of (unqiue in heartbeat increment) pages pings following a page ping (gap of heartbeat)
                count(distinct case
                        when event_name = 'page_ping' and page_view_id is not null then
                        -- need to get a unique list of floored time PER page view, so create a dummy surrogate key...
                            {{ dbt.concat(['page_view_id', "cast(floor("~snowplow_utils.to_unixtstamp('dvce_created_tstamp')~"/"~var('snowplow__heartbeat', 10)~") as "~snowplow_utils.type_max_string()~")" ]) }}
                        else
                            null end) -
                    count(distinct case when event_name = 'page_ping' and page_view_id is not null then page_view_id else null end)
                ))  +
            -- number of page pings following a page view (or no event) (gap of min visit length)
            (count(distinct case when event_name = 'page_ping' and page_view_id is not null then page_view_id else null end) * {{ var("snowplow__min_visit_length", 5) }}) as engaged_time_in_s
        , {{ snowplow_utils.timestamp_diff('min(derived_tstamp)', 'max(derived_tstamp)', 'second') }} as absolute_time_in_s
    from {{ ref('snowplow_unified_base_events_this_run') }}
    where
        1 = 1
        {% if var("snowplow__ua_bot_filter", true) %}
            {{ filter_bots() }}
        {% endif %}
    group by
        domain_sessionid
)

{# Redshift doesn't allow listagg and other aggregations in the same CTE #}
{%- if var('snowplow__conversion_events', none) %}
,session_convs as (
    select
        domain_sessionid
        {%- for conv_def in var('snowplow__conversion_events') %}
            {{ snowplow_unified.get_conversion_columns(conv_def)}}
        {%- endfor %}
    from {{ ref('snowplow_unified_base_events_this_run') }}
    where
        1 = 1
        {% if var("snowplow__ua_bot_filter", true) %}
            {{ filter_bots() }}
        {% endif %}
    group by
        domain_sessionid
)
{%- endif %}

select
    -- app id
    a.app_id,

    a.platform,

    -- session fields
    a.domain_sessionid,
    a.original_domain_sessionid,
    a.domain_sessionidx,

    -- when the session starts with a ping we need to add the min visit length to get when the session actually started
    case when a.event_name = 'page_ping' then
        {{ snowplow_utils.timestamp_add(datepart="second", interval=-var("snowplow__min_visit_length", 5), tstamp="c.start_tstamp") }}
    else c.start_tstamp end as start_tstamp,
    c.end_tstamp,
    a.model_tstamp,

    -- user fields
    a.user_id,
    a.domain_userid,
    a.original_domain_userid,
    a.stitched_user_id,
    a.network_userid,

    -- engagement fields
    c.page_views,
    c.engaged_time_in_s,
    {%- if var('snowplow__list_event_counts', false) %}
        {% if target.type in ['postgres'] %}
            cast(event_counts_string as json) as event_counts,
        {% elif target.type in ['redshift'] %}
            json_parse(event_counts_string) as event_counts,
        {% endif %}
    {% endif %}
    c.total_events,
    {{ engaged_session() }} as is_engaged,
    -- when the session starts with a ping we need to add the min visit length to get when the session actually started
    c.absolute_time_in_s + case when a.event_name = 'page_ping' then {{ var("snowplow__min_visit_length", 5) }} else 0 end as absolute_time_in_s,

    -- first page fields
    a.first_page_title,
    a.first_page_url,
    a.first_page_urlscheme,
    a.first_page_urlhost,
    a.first_page_urlpath,
    a.first_page_urlquery,
    a.first_page_urlfragment,

    -- only take the first value when the last is genuinely missing (base on url as has to always be populated)
    case when b.last_page_url is null then coalesce(b.last_page_title, a.first_page_title) else b.last_page_title end as last_page_title,
    case when b.last_page_url is null then coalesce(b.last_page_url, a.first_page_url) else b.last_page_url end as last_page_url,
    case when b.last_page_url is null then coalesce(b.last_page_urlscheme, a.first_page_urlscheme) else b.last_page_urlscheme end as last_page_urlscheme,
    case when b.last_page_url is null then coalesce(b.last_page_urlhost, a.first_page_urlhost) else b.last_page_urlhost end as last_page_urlhost,
    case when b.last_page_url is null then coalesce(b.last_page_urlpath, a.first_page_urlpath) else b.last_page_urlpath end as last_page_urlpath,
    case when b.last_page_url is null then coalesce(b.last_page_urlquery, a.first_page_urlquery) else b.last_page_urlquery end as last_page_urlquery,
    case when b.last_page_url is null then coalesce(b.last_page_urlfragment, a.first_page_urlfragment) else b.last_page_urlfragment end as last_page_urlfragment,

    -- referrer fields
    a.referrer,
    a.refr_urlscheme,
    a.refr_urlhost,
    a.refr_urlpath,
    a.refr_urlquery,
    a.refr_urlfragment,
    a.refr_medium,
    a.refr_source,
    a.refr_term,

    -- marketing fields
    a.mkt_medium,
    a.mkt_source,
    a.mkt_term,
    a.mkt_content,
    a.mkt_campaign,
    a.mkt_clickid,
    a.mkt_network,
    a.mkt_source_platform,
    a.default_channel_group,

    -- geo fields
    a.geo_country,
    a.geo_region,
    a.geo_region_name,
    a.geo_city,
    a.geo_zipcode,
    a.geo_latitude,
    a.geo_longitude,
    a.geo_timezone,
    a.geo_country_name,
    a.geo_continent,
    case when b.last_geo_country is null then coalesce(b.last_geo_country, a.geo_country) else b.last_geo_country end as last_geo_country,
    case when b.last_geo_country is null then coalesce(b.last_geo_region_name, a.geo_region_name) else b.last_geo_region_name end as last_geo_region_name,
    case when b.last_geo_country is null then coalesce(b.last_geo_city, a.geo_city) else b.last_geo_city end as last_geo_city,
    case when b.last_geo_country is null then coalesce(b.last_geo_country_name,a.geo_country_name) else b.last_geo_country_name end as last_geo_country_name,
    case when b.last_geo_country is null then coalesce(b.last_geo_continent, a.geo_continent) else b.last_geo_continent end as last_geo_continent,

    -- ip address
    a.user_ipaddress,

    -- user agent
    a.useragent,

    a.br_renderengine,
    a.br_lang,
    a.br_lang_name,
    case when b.last_br_lang is null then coalesce(b.last_br_lang, a.br_lang) else b.last_br_lang end as last_br_lang,
    case when b.last_br_lang is null then coalesce(b.last_br_lang_name, a.br_lang_name) else b.last_br_lang_name end as last_br_lang_name,

    a.os_timezone,

    -- optional fields, only populated if enabled.
    -- iab enrichment fields
    a.iab_category as category,
    a.iab_primary_impact as primary_impact,
    a.iab_reason as reason,
    a.iab_spider_or_robot as spider_or_robot,

    -- ua parser enrichment fields
    a.ua_useragent_family as useragent_family,
    a.ua_useragent_major as useragent_major,
    a.ua_useragent_minor as useragent_minor,
    a.ua_useragent_patch as useragent_patch,
    a.ua_useragent_version as useragent_version,
    a.ua_os_family as os_family,
    a.ua_os_major as os_major,
    a.ua_os_minor as os_minor,
    a.ua_os_patch as os_patch,
    a.ua_os_patch_minor as os_patch_minor,
    a.ua_os_version as os_version,
    a.ua_device_family as device_family,

    -- yauaa enrichment fields
    a.yauaa_device_class as device_class,
    case when a.yauaa_device_class = 'Desktop' THEN 'Desktop'
        when a.yauaa_device_class = 'Phone' then 'Mobile'
        when a.yauaa_device_class = 'Tablet' then 'Tablet'
        else 'Other' end as device_category,
    a.screen_resolution,
    a.yauaa_agent_class as agent_class,
    a.yauaa_agent_name as agent_name,
    a.yauaa_agent_name_version as agent_name_version,
    a.yauaa_agent_name_version_major as agent_name_version_major,
    a.yauaa_agent_version as agent_version,
    a.yauaa_agent_version_major as agent_version_major,
    a.yauaa_device_brand as device_brand,
    a.yauaa_device_name as device_name,
    a.yauaa_device_version as device_version,
    a.yauaa_layout_engine_class as layout_engine_class,
    a.yauaa_layout_engine_name as layout_engine_name,
    a.yauaa_layout_engine_name_version as layout_engine_name_version,
    a.yauaa_layout_engine_name_version_major as layout_engine_name_version_major,
    a.yauaa_layout_engine_version as layout_engine_version,
    a.yauaa_layout_engine_version_major as layout_engine_version_major,
    a.yauaa_operating_system_class as operating_system_class,
    a.yauaa_operating_system_name as operating_system_name,
    a.yauaa_operating_system_name_version as operating_system_name_version,
    a.yauaa_operating_system_version as operating_system_version

    -- conversion fields
    {%- if var('snowplow__conversion_events', none) %}
        {%- for conv_def in var('snowplow__conversion_events') %}
    {{ snowplow_unified.get_conversion_columns(conv_def, names_only = true)}}
        {%- endfor %}
    {% if var('snowplow__total_all_conversions', false) %}
    ,{%- for conv_def in var('snowplow__conversion_events') %}{{'cv_' ~ conv_def['name'] ~ '_volume'}}{%- if not loop.last %} + {% endif -%}{%- endfor %} as cv__all_volume
    {# Use 0 in case of no conversions having a value field #}
    ,0 {%- for conv_def in var('snowplow__conversion_events') %}{%- if conv_def.get('value') %} + {{'cv_' ~ conv_def['name'] ~ '_total'}}{% endif -%}{%- endfor %} as cv__all_total
    {% endif %}
    {%- endif %}

    -- passthrough fields
    {%- if var('snowplow__session_passthroughs', []) -%}
        {%- for col in passthrough_names %}
            , a.{{col}}
        {%- endfor -%}
    {%- endif %}
from
    session_firsts a
left join
    session_lasts b on a.domain_sessionid = b.domain_sessionid and b.page_event_in_session_index = 1
left join
    session_aggs c on a.domain_sessionid = c.domain_sessionid
{%- if var('snowplow__conversion_events', none) %}
left join
    session_convs d on a.domain_sessionid = d.domain_sessionid
{%- endif %}
where
    a.page_event_in_session_index = 1
