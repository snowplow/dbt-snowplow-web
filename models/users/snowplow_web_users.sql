{{ 
  config(
    materialized=var("snowplow__incremental_materialization"),
    unique_key='domain_userid',
    upsert_date_key='start_tstamp',
    disable_upsert_lookback=true,
    sort='start_tstamp',
    dist='domain_userid',
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=snowplow_web.cluster_by_fields_users(),
    tags=["derived"]
  ) 
}}

select * 
from {{ ref('snowplow_web_users_this_run') }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
