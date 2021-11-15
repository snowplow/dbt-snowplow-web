{{ 
  config(
    materialized=var("snowplow__incremental_materialization"),
    unique_key='domain_sessionid',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='domain_sessionid',
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=snowplow_web.cluster_by_fields_sessions(),
    tags=["derived"],
    post_hook="{{ snowplow_web.stitch_user_identifiers(
      enabled=var('snowplow__session_stitching')
      ) }}"
  ) 
}}


select * 
from {{ ref('snowplow_web_sessions_this_run') }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
