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
  original_domain_userid,
  cast(page_view_id as {{ dbt.type_string() }}) as page_view_id,
  domain_sessionid,
  original_domain_sessionid,
  cmp_load_time,
  cast(cmp_tstamp as {{ dbt.type_timestamp() }}) as cmp_tstamp,
  cast(first_consent_event_tstamp as {{ dbt.type_timestamp() }}) as first_consent_event_tstamp,
  first_consent_event_type,
  cmp_interaction_time

from {{ ref('snowplow_unified_consent_cmp_stats_expected') }}
