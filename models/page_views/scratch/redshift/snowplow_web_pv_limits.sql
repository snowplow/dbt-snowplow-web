-- Materialize as table since limits used in many subsequent queries.
{{ 
  config(
    materialized='table'
  ) 
}}

select
  min(collector_tstamp) as lower_limit,
  max(collector_tstamp) as upper_limit

from {{ ref('snowplow_web_base_events_this_run') }}

where page_view_id is not null
