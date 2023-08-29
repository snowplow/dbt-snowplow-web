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

  event_id,
  domain_userid,
  user_id,
  geo_country,
  page_view_id,
  domain_sessionid,
  derived_tstamp,
  load_tstamp,
  event_name,
  event_type,
  basis_for_processing,
  consent_url,
  consent_version,
  consent_scopes,
  domains_applied,
  gdpr_applies,
  cmp_load_time

from {{ ref('snowplow_unified_consent_log') }}
