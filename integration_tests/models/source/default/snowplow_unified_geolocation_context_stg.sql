-- test dataset includes contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0 due to a mutual solution covering other adapters
-- needs to be separated into its own table here

with prep as (

  select
    event_id as root_id,
    collector_tstamp as root_tstamp,
    -- get the start as where that text starts + length text + 1 ("": ).
    -- get end as start of next text minus start -1
    -- cursed string end and : check just in case there is ever an earlier match of the same word.
    substring(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0,
      position( 'latitude":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('latitude":') + 1,
        position( 'longitude":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ -(position( 'latitude":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('latitude":') + 1) -2
        ) as latitude,
    substring(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0,
      position( 'longitude":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('longitude":') + 1,
        position( 'latitude_longitude_accuracy":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)- (position( 'longitude":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('longitude":') + 1) -2
        ) as longitude,
    substring(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0,
      position( 'latitude_longitude_accuracy":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('latitude_longitude_accuracy":') + 1,
        position( 'altitude":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0) - (position( 'latitude_longitude_accuracy":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('latitude_longitude_accuracy":') + 1) -2
        ) as latitude_longitude_accuracy,
    substring(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0,
      position( 'altitude":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('altitude":') + 1,
        position( 'altitude_accuracy":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0) - (position( 'altitude":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('altitude":') + 1) -2
        ) as altitude,
    substring(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0,
      position( 'altitude_accuracy":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('altitude_accuracy":') + 1,
        position( 'bearing":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0) - (position( 'altitude_accuracy":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('altitude_accuracy":') + 1) -2
        ) as altitude_accuracy,
    substring(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0,
      position( 'bearing":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('bearing":') + 1,
        position( 'speed":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0) - (position( 'bearing":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('bearing":') + 1) -2
        ) as bearing,
    substring(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0,
      position( 'speed":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('speed":') + 1,
        position( 'timestamp' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0) - (position( 'speed":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('speed":') + 1) -2
    ) as speed,
    substring(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0,
      position( 'timestamp":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('timestamp":') + 1,
        length(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0) - 1 - (position( 'timestamp":' in contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0)+ length('timestamp":') + 1)
        ) as timestamp
  from {{ ref("snowplow_unified_events") }}

)

select
    root_id,
    root_tstamp,
    'geolocation_context' as schema_name,
    case when latitude = 'null' then null else latitude::float end as latitude,
    case when longitude = 'null' then null else longitude::float end as longitude,
    case when latitude_longitude_accuracy = 'null' then null else latitude_longitude_accuracy::float end as latitude_longitude_accuracy,
    case when altitude = 'null' then null else altitude::float end as altitude,
    case when altitude_accuracy = 'null' then null else altitude_accuracy::float end as altitude_accuracy,
    case when bearing = 'null' then null else bearing::float end as bearing,
    case when speed = 'null' then null else speed::float end as speed,
    case when timestamp = 'null' then null else timestamp::INTEGER end as timestamp

from prep
