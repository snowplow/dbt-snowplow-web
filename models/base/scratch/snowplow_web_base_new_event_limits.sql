{{ config(
   pre_hook=before_begin("{{ snowplow_utils.snowplow_incremental_pre_hook('snowplow_web') }}")) 
}}

select * from {{ snowplow_utils.get_current_incremental_tstamp_table_relation('snowplow_web') }}
