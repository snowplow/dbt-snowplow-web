{{ 
  config(
    materialized=var("snowplow__incremental_materialization"),
    unique_key='page_view_id',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='page_view_id',
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp"
    },
    cluster_by=snowplow_web.cluster_by_fields_page_views(),
    tags=["derived"]
  ) 
}}


select * 
from {{ ref('snowplow_web_page_views_this_run') }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
