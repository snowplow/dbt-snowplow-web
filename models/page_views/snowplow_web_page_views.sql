{{ 
  config(
    enabled=var('snowplow__enable_derived_page_views'),
    materialized='snowplow_incremental',
    unique_key='page_view_id',
    upsert_date_key='start_tstamp',
    sort='start_tstamp',
    dist='page_view_id'
  ) 
}}


select * from {{ ref('snowplow_web_page_views_this_run') }}
