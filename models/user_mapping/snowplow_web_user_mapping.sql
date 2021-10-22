{{ 
  config(
    materialized='incremental',
    unique_key='domain_userid',
    sort='end_tstamp',
    dist='domain_userid',
    partition_by = {
      "field": "end_tstamp",
      "data_type": "timestamp"},
    tags=["derived"]
  ) 
}}


select distinct
  domain_userid,
  last_value(user_id) over(
    partition by domain_userid 
    order by collector_tstamp 
    rows between unbounded preceding and unbounded following
  ) as user_id,
  max(collector_tstamp) over (partition by domain_userid) as end_tstamp

from {{ ref('snowplow_web_base_events_this_run') }}

where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
and user_id is not null
and domain_userid is not null
