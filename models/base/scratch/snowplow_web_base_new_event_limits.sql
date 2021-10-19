{{ config(
   materialized="table",
   post_hook=["{{snowplow_utils.print_run_limits(this)}}"]
   )
}}


{%- set models_in_run = snowplow_utils.get_enabled_snowplow_models('snowplow_web') -%}

{% set min_last_success,
         max_last_success, 
         models_matched_from_manifest,
         has_matched_all_models = snowplow_utils.get_incremental_manifest_status(ref('snowplow_web_incremental_manifest'),
                                                                                 models_in_run) -%}


{% set run_limits_query = snowplow_utils.get_run_limits(min_last_success, 
                                                          max_last_success,
                                                          models_matched_from_manifest,
                                                          has_matched_all_models,
                                                          var("snowplow__start_date","2020-01-01")) -%}


{{ run_limits_query }}
