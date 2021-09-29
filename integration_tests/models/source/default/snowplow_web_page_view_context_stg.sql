-- test dataset includes page_view_id as part of events table. 
-- RS and PG events tables are federated so split out page_view_id into its own table

with prep as (
select
  event_id as root_id,
  collector_tstamp as root_tstamp,
  split_part(split_part(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,'[{"id":"', 2), '"}]', 1) as id -- test dataset uses json format. Extract.

from {{ ref('snowplow_web_events') }}
where {{ edge_cases_to_ignore() }} --filter out any edge cases we havent yet solved for but are included in the test dataset.
)

select 
  root_id,
  root_tstamp,
  case when id = 'null' or id = '' then null else id end as id

from prep

