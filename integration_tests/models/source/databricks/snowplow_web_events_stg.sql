-- page view context is given as json string in csv. Parse json
with prep as (
select
  *,
  from_json(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0, 'array<struct<id:string>>') as contexts_com_snowplowanalytics_snowplow_web_page_1

from {{ ref('snowplow_web_events') }}
)


select
  * 

from prep

where {{ edge_cases_to_ignore() }} --filter out any edge cases we havent yet solved for but are included in the test dataset.
