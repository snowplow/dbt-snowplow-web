{{
    config(
        tags=["this_run"]
    )
}}

with session_firsts as (
    select
        -- app id
        app_id as app_id,

        -- session fields
        domain_sessionid,
        domain_sessionidx,

        {{ snowplow_utils.current_timestamp_in_utc() }} as model_tstamp,

        -- user fields
        user_id,
        domain_userid,
        {% if var('snowplow__session_stitching') %}
            -- updated with mapping as part of post hook on derived sessions table
            cast(domain_userid as {{ type_string() }}) as stitched_user_id,
        {% else %}
            cast(null as {{ type_string() }}) as stitched_user_id,
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
        regexp_extract(page_url ,r'utm_source_platform=([^?&#]*)') as mkt_source_platform,
        case
            when mkt_source = '(direct)' and mkt_medium in ('(not set)', '(none)') then 'Direct'
            when CONTAINS_SUBSTR(mkt_medium, 'cross-network') then 'Cross-network'
            when REGEXP_CONTAINS(mkt_medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') then
            case
                when mkt_source in ({{select_source_category('SOURCE_CATEGORY_SHOPPING')}}) or REGEXP_CONTAINS(mkt_campaign, r'^(.*(([^a-df-z]|^)shop|shopping).*)$)') then 'Paid Shopping'
                when mkt_source in ({{select_source_category('SOURCE_CATEGORY_SEARCH')}}) then 'Paid Search'
                when mkt_source in ({{select_source_category('SOURCE_CATEGORY_SOCIAL')}}) then 'Paid Social'
                when mkt_source in ({{select_source_category('SOURCE_CATEGORY_VIDEO')}}) then 'Paid Video'
                else 'Paid Other'
            end
            when mkt_medium in ('display', 'banner', 'expandable', 'intersitial', 'cpm') then 'Display'
            when mkt_source in ({{select_source_category('SOURCE_CATEGORY_SHOPPING')}}) or REGEXP_CONTAINS(mkt_campaign, r'^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Organic Shopping'
            when mkt_source in ({{select_source_category('SOURCE_CATEGORY_SOCIAL')}}) or mkt_medium in ('social', 'social-network', 'sm', 'social network', 'social media') then 'Organic Social'
            when mkt_source in ({{select_source_category('SOURCE_CATEGORY_VIDEO')}}) or REGEXP_CONTAINS(mkt_medium, r'^(.*video.*)$') then 'Organic Video'
            when mkt_source in ({{select_source_category('SOURCE_CATEGORY_SEARCH')}}) or mkt_medium = 'organic' then 'Organic Search'
            when mkt_medium in ('referral', 'app', 'link') then 'Referral'
            when mkt_source in ('email', 'e-mail', 'e_mail', 'e mail') OR mkt_medium in ('email', 'e-mail', 'e_mail', 'e mail') then 'Email'
            when mkt_medium = 'affiliate' then 'Affiliates'
            when mkt_medium = 'audio' then 'Audio'
            when mkt_source = 'sms' or mkt_medium = 'sms' then 'SMS'
            when ENDS_WITH(mkt_medium, 'push') or REGEXP_CONTAINS(mkt_medium, r'.*(mobile|notification).*') or mkt_source = 'firebase' then 'Mobile Push Notifications'
        end as default_channel

        -- Most if not all the following fields should be the same across all events in a session, but this ensures they are
        -- geo fields
        geo_country as geo_country,
        geo_region as geo_region,
        geo_region_name as geo_region_name,
        geo_city as geo_city,
        geo_zipcode as geo_zipcode,
        geo_latitude as geo_latitude,
        geo_longitude as geo_longitude,
        geo_timezone as geo_timezone,

        -- ip address
        user_ipaddress as user_ipaddress,

        -- user agent
        useragent as useragent,

        br_renderengine as br_renderengine,
        br_lang as br_lang,
        os_timezone as os_timezone,

        -- optional fields, only populated if enabled.
        -- iab enrichment fields: set iab variable to true to enable
        {{ snowplow_utils.get_optional_fields(
                enabled=var('snowplow__enable_iab', false),
                fields=iab_fields(),
                col_prefix='contexts_com_iab_snowplow_spiders_and_robots_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='ev') }},

        -- ua parser enrichment fields: set ua_parser variable to true to enable
        {{ snowplow_utils.get_optional_fields(
                enabled=var('snowplow__enable_ua', false),
                fields=ua_fields(),
                col_prefix='contexts_com_snowplowanalytics_snowplow_ua_parser_context_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='ev') }},

        -- yauaa enrichment fields: set yauaa variable to true to enable
        {{ snowplow_utils.get_optional_fields(
                enabled=var('snowplow__enable_yauaa', false),
                fields=yauaa_fields(),
                col_prefix='contexts_nl_basjes_yauaa_context_1',
                relation=ref('snowplow_web_base_events_this_run'),
                relation_alias='ev') }},

        row_number() over (partition by ev.domain_sessionid order by ev.derived_tstamp, ev.dvce_created_tstamp) AS page_event_in_session_index,
        event_name
    from {{ ref('snowplow_web_base_events_this_run') }} ev
    where
        event_name in ('page_ping', 'page_view')
        and page_view_id is not null
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
        row_number() over (partition by domain_sessionid order by derived_tstamp desc, dvce_created_tstamp) AS page_event_in_session_index
    from {{ ref('snowplow_web_base_events_this_run') }}
    where
        event_name in ('page_view')
        and page_view_id is not null
        {% if var("snowplow__ua_bot_filter", true) %}
            {{ filter_bots() }}
        {% endif %}
),

session_aggs as (
    select
        domain_sessionid,
        min(derived_tstamp) as start_tstamp,
        max(derived_tstamp) as end_tstamp,
        -- engagement fields
        count(distinct page_view_id) as page_views,
        -- (hb * (#page pings - # distinct page view ids ON page pings)) + (# distinct page view ids ON page pings * min visit length)
        ({{ var("snowplow__heartbeat", 10) }} * (
                -- number of (unqiue in heartbeat increment) pages pings following a page ping (gap of heartbeat)
                count(distinct case
                        when event_name = 'page_ping' then
                        -- need to get a unique list of floored time PER page view, so create a dummy surrogate key...
                            {{ dbt.concat(['page_view_id', "cast(floor("~snowplow_utils.to_unixtstamp('dvce_created_tstamp')~"/"~var('snowplow__heartbeat', 10)~") as "~dbt.type_string()~")" ]) }}
                        else
                            null end) -
                    count(distinct case when event_name = 'page_ping' then page_view_id else null end)
                ))  +
            -- number of page pings following a page view (or no event) (gap of min visit length)
            (count(distinct case when event_name = 'page_ping' then page_view_id else null end) * {{ var("snowplow__min_visit_length", 5) }}) as engaged_time_in_s,
        {{ snowplow_utils.timestamp_diff('min(derived_tstamp)', 'max(derived_tstamp)', 'second') }} as absolute_time_in_s,
        -- number of page view ids with only page pings...
    from {{ ref('snowplow_web_base_events_this_run') }}
    where
        event_name in ('page_ping', 'page_view')
        and page_view_id is not null
        {% if var("snowplow__ua_bot_filter", true) %}
            {{ filter_bots() }}
        {% endif %}
    group by
        domain_sessionid
),

event_counts as (
    select
        domain_sessionid,
        array_agg(struct(
            name,
            counts
        )) as event_counts
    from (
        select
            domain_sessionid,
            event_name as name,
            count(*) as counts
        from {{ ref('snowplow_web_base_events_this_run') }}
        where domain_sessionid is not null
        and event_name is not null
        group by
            domain_sessionid, event_name
    )
    group by
        domain_sessionid
)

select
    -- app id
    a.app_id,

    -- session fields
    a.domain_sessionid,
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
    a.stitched_user_id,
    a.network_userid,

    -- engagement fields
    c.page_views,
    c.engaged_time_in_s,
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

    -- geo fields
    a.geo_country,
    a.geo_region,
    a.geo_region_name,
    a.geo_city,
    a.geo_zipcode,
    a.geo_latitude,
    a.geo_longitude,
    a.geo_timezone,

    -- ip address
    a.user_ipaddress,

    -- user agent
    a.useragent,

    a.br_renderengine,
    a.br_lang,

    a.os_timezone,

    -- optional fields, only populated if enabled.
    -- iab enrichment fields
    a.category,
    a.primary_impact,
    a.reason,
    a.spider_or_robot,

    -- ua parser enrichment fields
    a.useragent_family,
    a.useragent_major,
    a.useragent_minor,
    a.useragent_patch,
    a.useragent_version,
    a.os_family,
    a.os_major,
    a.os_minor,
    a.os_patch,
    a.os_patch_minor,
    a.os_version,
    a.device_family,

    -- yauaa enrichment fields
    a.device_class,
    a.agent_class,
    a.agent_name,
    a.agent_name_version,
    a.agent_name_version_major,
    a.agent_version,
    a.agent_version_major,
    a.device_brand,
    a.device_name,
    a.device_version,
    a.layout_engine_class,
    a.layout_engine_name,
    a.layout_engine_name_version,
    a.layout_engine_name_version_major,
    a.layout_engine_version,
    a.layout_engine_version_major,
    a.operating_system_class,
    a.operating_system_name,
    a.operating_system_name_version,
    a.operating_system_version

    -- count of all events per session
    d.event_counts
from
    session_firsts a
left join
    session_lasts b on a.domain_sessionid = b.domain_sessionid and b.page_event_in_session_index = 1
left join
    session_aggs c on a.domain_sessionid = c.domain_sessionid
left join
    event_counts d on a.domain_sessionid = d.domain_sessionid
where
    a.page_event_in_session_index = 1
