{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

select
  root_id,
  root_tstamp::timestamp,
  'yauaa' as schema_name,
  agentClass::varchar as agent_class,
  agentInformationEmail::varchar as agent_information_email,
  agentName::varchar as agent_name,
  agentNameVersion::varchar as agent_name_version,
  agentNameVersionMajor::varchar as agent_name_version_major,
  agentVersion::varchar as agent_version,
  agentVersionMajor::varchar as agent_version_major,
  deviceBrand::varchar as device_brand,
  deviceClass::varchar as device_class,
  deviceCpu::varchar as device_cpu,
  deviceCpuBits::varchar as device_cpu_bits,
  deviceName::varchar as device_name,
  deviceVersion::varchar as device_version,
  layoutEngineClass::varchar as layout_engine_class,
  layoutEngineName::varchar as layout_engine_name,
  layoutEngineNameVersion::varchar as layout_engine_name_version,
  layoutEngineNameVersionMajor::varchar as layout_engine_name_version_major,
  layoutEngineVersion::varchar as layout_engine_version,
  layoutEngineVersionMajor::varchar as layout_engine_version_major,
  networkType::varchar as network_type,
  operatingSystemClass::varchar as operating_system_class,
  operatingSystemName::varchar as operating_system_name,
  operatingSystemNameVersion::varchar as operating_system_name_version,
  operatingSystemNameVersionMajor::varchar as operating_system_name_version_major,
  operatingSystemVersion::varchar as operating_system_version,
  operatingSystemVersionBuild::varchar as operating_system_version_build,
  operatingSystemVersionMajor::varchar as operating_system_version_major,
  webviewAppName::varchar as webview_app_name,
  webviewAppNameVersionMajor::varchar as webview_app_name_version_major,
  webviewAppVersion::varchar as webview_app_version,
  webviewAppVersionMajor::varchar as webview_app_version_major

from {{ ref('snowplow_unified_yauaa_context') }}
