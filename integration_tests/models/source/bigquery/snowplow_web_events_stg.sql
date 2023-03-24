-- page view context is given as json string in csv. Extract array from json
with prep as (
select
  *
  except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0, contexts_com_iab_snowplow_spiders_and_robots_1_0_0, contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0, contexts_nl_basjes_yauaa_context_1_0_0),
  JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) AS contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,
  JSON_EXTRACT_ARRAY(unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0) AS unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0,
  JSON_EXTRACT_ARRAY(unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0) AS unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0,
  JSON_EXTRACT_ARRAY(contexts_com_iab_snowplow_spiders_and_robots_1_0_0) as contexts_com_iab_snowplow_spiders_and_robots_1_0_0,
  JSON_EXTRACT_ARRAY(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0) as contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0,
  JSON_EXTRACT_ARRAY(contexts_nl_basjes_yauaa_context_1_0_0) as contexts_nl_basjes_yauaa_context_1_0_0
from {{ ref('snowplow_web_events') }}
)

-- recreate repeated record field i.e. array of structs as is originally in BQ events table
select
  *
  except(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0, contexts_com_iab_snowplow_spiders_and_robots_1_0_0, contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0, contexts_nl_basjes_yauaa_context_1_0_0),
  array(
    select as struct
      JSON_EXTRACT_scalar(json_array,'$.id') as id
    from unnest(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_web_page_1_0_0,

  array(
    select as struct
      JSON_EXTRACT_scalar(json_array,'$.basis_for_processing') as basis_for_processing,
      JSON_EXTRACT_STRING_ARRAY(json_array,'$.consent_scopes') as consent_scopes,
      JSON_EXTRACT_scalar(json_array,'$.consent_url') as consent_url,
      JSON_EXTRACT_scalar(json_array,'$.consent_version') as consent_version,
      JSON_EXTRACT_STRING_ARRAY(json_array,'$.domains_applied') as domains_applied,
      JSON_EXTRACT_scalar(json_array,'$.event_type') as event_type,
      JSON_EXTRACT_scalar(json_array,'$.gdpr_applies') as gdpr_applies
    from unnest(unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0) as json_array
    ) as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0,

  array(
    select as struct
      JSON_EXTRACT_scalar(json_array,'$.elapsed_time') as elapsed_time
    from unnest(unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0) as json_array
    ) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0,

  array(
    select as struct
      JSON_EXTRACT_scalar(json_array,'$.category') as category,
      JSON_EXTRACT_scalar(json_array,'$.primaryImpact') as primary_impact,
      JSON_EXTRACT_scalar(json_array,'$.reason') as reason,
      cast(JSON_EXTRACT_scalar(json_array ,'$.spiderOrRobot') as boolean) as spider_or_robot
    from unnest(contexts_com_iab_snowplow_spiders_and_robots_1_0_0) as json_array
    ) as contexts_com_iab_snowplow_spiders_and_robots_1_0_0,

  array(
    select as struct
      JSON_EXTRACT_scalar(json_array,'$.deviceFamily') as device_family,
      JSON_EXTRACT_scalar(json_array,'$.osFamily') as os_family,
      JSON_EXTRACT_scalar(json_array,'$.osMajor') as os_major,
      JSON_EXTRACT_scalar(json_array,'$.osMinor') as os_minor,
      JSON_EXTRACT_scalar(json_array,'$.osPatch') as os_patch,
      JSON_EXTRACT_scalar(json_array,'$.osPatchMinor') as os_patch_minor,
      JSON_EXTRACT_scalar(json_array,'$.osVersion') as os_version,
      JSON_EXTRACT_scalar(json_array,'$.useragentFamily') as useragent_family,
      JSON_EXTRACT_scalar(json_array,'$.useragentMajor') as useragent_major,
      JSON_EXTRACT_scalar(json_array,'$.useragentMinor') as useragent_minor,
      JSON_EXTRACT_scalar(json_array,'$.useragentPatch') as useragent_patch,
      JSON_EXTRACT_scalar(json_array,'$.useragentVersion') as useragent_version
    from unnest(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0) as json_array
    ) as contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0,

  array(
    select as struct
      JSON_EXTRACT_scalar(json_array,'$.agentClass') as agent_class,
      JSON_EXTRACT_scalar(json_array,'$.agentInformationEmail') as agent_information_email,
      JSON_EXTRACT_scalar(json_array,'$.agentName') as agent_name,
      JSON_EXTRACT_scalar(json_array,'$.agentNameVersion') as agent_name_version,
      JSON_EXTRACT_scalar(json_array,'$.agentNameVersionMajor') as agent_name_version_major,
      JSON_EXTRACT_scalar(json_array,'$.agentVersion') as agent_version,
      JSON_EXTRACT_scalar(json_array,'$.agentVersionMajor') as agent_version_major,
      JSON_EXTRACT_scalar(json_array,'$.deviceBrand') as device_brand,
      JSON_EXTRACT_scalar(json_array,'$.deviceClass') as device_class,
      JSON_EXTRACT_scalar(json_array,'$.deviceCpu') as device_cpu,
      JSON_EXTRACT_scalar(json_array,'$.deviceCpuBits') as device_cpu_bits,
      JSON_EXTRACT_scalar(json_array,'$.deviceName') as device_name,
      JSON_EXTRACT_scalar(json_array,'$.deviceVersion') as device_version,
      JSON_EXTRACT_scalar(json_array,'$.layoutEngineClass') as layout_engine_class,
      JSON_EXTRACT_scalar(json_array,'$.layoutEngineName') as layout_engine_name,
      JSON_EXTRACT_scalar(json_array,'$.layoutEngineNameVersion') as layout_engine_name_version,
      JSON_EXTRACT_scalar(json_array,'$.layoutEngineNameVersionMajor') as layout_engine_name_version_major,
      JSON_EXTRACT_scalar(json_array,'$.layoutEngineVersion') as layout_engine_version,
      JSON_EXTRACT_scalar(json_array,'$.layoutEngineVersionMajor') as layout_engine_version_major,
      JSON_EXTRACT_scalar(json_array,'$.networkType') as network_type,
      JSON_EXTRACT_scalar(json_array,'$.operatingSystemClass') as operating_system_class,
      JSON_EXTRACT_scalar(json_array,'$.operatingSystemName') as operating_system_name,
      JSON_EXTRACT_scalar(json_array,'$.operatingSystemNameVersion') as operating_system_name_version,
      JSON_EXTRACT_scalar(json_array,'$.operatingSystemNameVersionMajor') as operating_system_name_version_major,
      JSON_EXTRACT_scalar(json_array,'$.operatingSystemVersion') as operating_system_version,
      JSON_EXTRACT_scalar(json_array,'$.operatingSystemVersionBuild') as operating_system_version_build,
      JSON_EXTRACT_scalar(json_array,'$.operatingSystemVersionMajor') as operating_system_version_major,
      JSON_EXTRACT_scalar(json_array,'$.webviewAppName') as webview_app_name,
      JSON_EXTRACT_scalar(json_array,'$.webviewAppNameVersionMajor') as webview_app_name_version_major,
      JSON_EXTRACT_scalar(json_array,'$.webviewAppVersion') as webview_app_version,
      JSON_EXTRACT_scalar(json_array,'$.webviewAppVersionMajor') as webview_app_version_major
    from unnest(contexts_nl_basjes_yauaa_context_1_0_0) as json_array
    ) as contexts_nl_basjes_yauaa_context_1_0_0,


from prep
