{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    enabled=var("snowplow__enable_consent", false)
    )
}}

select

  replace(cast(consent_version as {{ dbt.type_string() }}), '.0', '') ||'.0' as consent_version,
  cast(version_start_tstamp as {{ dbt.type_timestamp() }}) as version_start_tstamp,
  consent_scopes,
  consent_url,
  domains_applied,
  cast(is_latest_version as {{ dbt.type_boolean() }}) as is_latest_version,
  cast(last_allow_all_event as {{ dbt.type_timestamp() }}) as last_allow_all_event

from {{ ref('snowplow_unified_consent_versions_expected') }}
