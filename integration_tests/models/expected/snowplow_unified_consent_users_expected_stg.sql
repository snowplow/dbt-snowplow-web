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

  domain_userid,
  user_id,
  geo_country,
  cmp_events,
  consent_events,
  cast(last_cmp_event_tstamp as {{ dbt.type_timestamp() }}) as last_cmp_event_tstamp,
  cast(last_consent_event_tstamp as {{ dbt.type_timestamp() }}) as last_consent_event_tstamp,
  last_consent_event_type,
  last_consent_scopes,
  replace(cast(last_consent_version as {{ dbt.type_string() }}), '.0', '') ||'.0' as last_consent_version,
  last_consent_url,
  last_domains_applied,
  cast(last_processed_event as {{ dbt.type_timestamp() }}) as last_processed_event,
  cast(is_latest_version as {{ dbt.type_boolean() }})  as is_latest_version

from {{ ref('snowplow_unified_consent_users_expected') }}
