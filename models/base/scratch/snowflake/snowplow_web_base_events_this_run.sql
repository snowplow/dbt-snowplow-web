{{ 
  config(
    materialized='table',
    tags=["this_run"]
  ) 
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref('snowplow_web_base_sessions_this_run'),
                                                                          'start_tstamp',
                                                                          'end_tstamp') %}

{%- set columns = adapter.get_columns_in_relation(var('snowplow__events'))|map(attribute='column')|map('lower')|list -%}
{# No 'EXCEPT' to exclude cols in Snowflake. Remove domain_userid and page view context from returned columns then render. #}
{%- set filtered_columns = columns|reject("equalto","domain_userid")|reject("equalto","contexts_com_snowplowanalytics_snowplow_web_page_1") -%}


select
  a.contexts_com_snowplowanalytics_snowplow_web_page_1[0]:id::varchar as page_view_id,
  b.domain_userid, -- take domain_userid from manifest. This ensures only 1 domain_userid per session.
  {{ 'a.'~filtered_columns|join(',\n\ta.') }}

from {{ var('snowplow__events') }} as a
inner join {{ ref('snowplow_web_base_sessions_this_run') }} as b
on a.domain_sessionid = b.session_id

where a.collector_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'b.start_tstamp') }}
and a.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'a.dvce_created_tstamp') }}
and a.collector_tstamp >= {{ lower_limit }}
and a.collector_tstamp <= {{ upper_limit }}
and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}

qualify row_number() over (partition by a.event_id order by a.collector_tstamp) = 1

