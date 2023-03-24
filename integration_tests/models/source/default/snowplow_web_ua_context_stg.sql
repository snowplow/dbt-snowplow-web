select
  root_id,
  root_tstamp::timestamp,
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

from {{ ref('snowplow_web_ua_context') }}
