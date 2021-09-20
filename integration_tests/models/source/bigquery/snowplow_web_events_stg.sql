-- page view context is given as json string in csv. Extract array from json
with prep as (
select
  *
  except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0),
  JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) AS contexts_com_snowplowanalytics_snowplow_web_page_1_0_0

from {{ ref('snowplow_web_events') }}
)

-- recreate repeated record field i.e. array of structs as is originally in BQ events table
select
  * except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0),
  array(
    select as struct JSON_EXTRACT_scalar(json_array,'$.id') as id 
    from unnest(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0

from prep

where {{ edge_cases_to_ignore() }} --filter out any edge cases we havent yet solved for but are included in the test dataset.
