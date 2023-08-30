-- test dataset includes unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0 due to a mutual solution covering other adapters
-- needs to be separated into its own table here

with prep as (
  select
    event_id as root_id,
    collector_tstamp as root_tstamp,
    substring(unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0, position( 'id' in unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0)+ 5, 36) as id, -- test dataset uses json format. Extract.
    'na' as name,
    'na' as previous_id,
    'na' as previous_name,
    'na' as previous_type,
    'na' as transition_type,
    'na' as type

  from {{ ref("snowplow_unified_events") }}

)

select
  root_id,
  root_tstamp,
  case when id = 'null' or id = '' then null else id end as id,
  name,
  previous_id,
  previous_name,
  previous_type,
  transition_type,
  type,
  'screen_view_context' as schema_name

from prep

