{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

select
  root_id,
  root_tstamp::timestamp,
  'ua_parser' as schema_name,
  deviceFamily::varchar as device_family,
  osFamily::varchar as os_family,
  osMajor::varchar as os_major,
  osMinor::varchar as os_minor,
  osPatch::varchar as os_patch,
  osPatchMinor::varchar as os_patch_minor,
  osVersion::varchar as os_version,
  useragentFamily::varchar as useragent_family,
  useragentMajor::varchar as useragent_major,
  useragentMinor::varchar as useragent_minor,
  useragentPatch::varchar as useragent_patch,
  useragentVersion::varchar as useragent_version

from {{ ref('snowplow_unified_ua_context') }}
