-- test dataset includes contexts_com_snowplowanalytics_snowplow_client_session_1_0_0 due to a mutual solution covering other adapters
-- needs to be separated into its own table here

with prep as (

  select
    event_id as root_id,
    collector_tstamp as root_tstamp,
    'session_context' as schema_name,

  {%- if target.type == 'postgres' %}
    substring(contexts_com_snowplowanalytics_snowplow_client_session_1_0_0, position( 'session_id' in contexts_com_snowplowanalytics_snowplow_client_session_1_0_0)+ 13, 36) as session_id, -- test dataset uses json format. Extract.

  {%- else -%}
    substring(contexts_com_snowplowanalytics_snowplow_client_session_1_0_0, position( 'session_id' in contexts_com_snowplowanalytics_snowplow_client_session_1_0_0)+ 13, 36) as session_id, -- test dataset uses json format. Extract.

  {%- endif %}

    case when right(substring(contexts_com_snowplowanalytics_snowplow_client_session_1_0_0, position( 'session_index' in contexts_com_snowplowanalytics_snowplow_client_session_1_0_0)+ 16, 3), 1) = ','
      then substring(contexts_com_snowplowanalytics_snowplow_client_session_1_0_0, position( 'session_index' in contexts_com_snowplowanalytics_snowplow_client_session_1_0_0)+ 16, 1)
      else substring(contexts_com_snowplowanalytics_snowplow_client_session_1_0_0, position( 'session_index' in contexts_com_snowplowanalytics_snowplow_client_session_1_0_0)+ 16, 2)
      end as session_index,
    substring(contexts_com_snowplowanalytics_snowplow_client_session_1_0_0, position( 'previous_session_id' in contexts_com_snowplowanalytics_snowplow_client_session_1_0_0)+ 22, 36) as previous_session_id,
    substring(contexts_com_snowplowanalytics_snowplow_client_session_1_0_0, position( 'user_id' in contexts_com_snowplowanalytics_snowplow_client_session_1_0_0)+ 10, 36) as user_id,
    substring(contexts_com_snowplowanalytics_snowplow_client_session_1_0_0, position( 'first_event_id' in contexts_com_snowplowanalytics_snowplow_client_session_1_0_0)+ 17, 36) as first_event_id

  from {{ ref("snowplow_unified_events") }}

)

select * from prep
