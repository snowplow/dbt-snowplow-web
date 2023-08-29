{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    materialized='table',
    enabled=var("snowplow__enable_consent", false)
  )
}}

with arrays as (

  select
    u.domain_userid,
    {{ snowplow_utils.get_split_to_array('last_consent_scopes', 'u', ', ') }} as scope_array

  from {{ ref('snowplow_unified_consent_users') }} u

  where is_latest_version

  )

  , unnesting as (

    {{ snowplow_utils.unnest('domain_userid', 'scope_array', 'consent_scope', 'arrays') }}

  )

select
  replace(replace(replace(cast(consent_scope as {{ snowplow_utils.type_max_string() }}), '"', ''), '[', ''), ']', '') as scope,
  count(*) as total_consent

from unnesting

group by 1
