{{ 
  config(
    tags=["this_run"]
  ) 
}}

--TODO:Add tests 

{%- set lower_limit, upper_limit, session_lookback_limit = snowplow_utils.return_base_new_event_limits(ref('snowplow_web_base_new_event_limits')) %}

select
  s.session_id,
  s.start_tstamp,
  s.end_tstamp

from {{ ref('snowplow_web_base_sessions_lifecycle_manifest')}} s

where 
   (s.end_tstamp between {{ lower_limit }} and {{ upper_limit }})
or (s.start_tstamp between {{ lower_limit }} and {{ upper_limit }})
