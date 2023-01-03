-- page view context is given as json string in csv. Extract array from json
with prep as (
select
  *
  except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0),
  JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) AS contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
  JSON_EXTRACT_ARRAY(unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0) AS unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0,
  JSON_EXTRACT_ARRAY(unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0) AS unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0

from {{ ref('snowplow_web_events') }}
)

-- recreate repeated record field i.e. array of structs as is originally in BQ events table
select
  *
  except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0),
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.id') as id
    from unnest(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,

  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.basis_for_processing') as basis_for_processing,
                    JSON_EXTRACT_STRING_ARRAY(json_array,'$.consent_scopes') as consent_scopes,
                    JSON_EXTRACT_scalar(json_array,'$.consent_url') as consent_url,
                    JSON_EXTRACT_scalar(json_array,'$.consent_version') as consent_version,
                    JSON_EXTRACT_STRING_ARRAY(json_array,'$.domains_applied') as domains_applied,
                    JSON_EXTRACT_scalar(json_array,'$.event_type') as event_type,
                    JSON_EXTRACT_scalar(json_array,'$.gdpr_applies') as gdpr_applies
    from unnest(unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0) as json_array
    ) as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0,

  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.elapsed_time') as elapsed_time
    from unnest(unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0) as json_array
    ) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0


from prep
