{{ config(
   pre_hook="{{ snowplow_utils.snowplow_incremental_pre_hook('snowplow_web') }}",
   materialized="table"
   )
}}

select * from {{ snowplow_utils.get_current_incremental_tstamp_table_relation('snowplow_web') }}
