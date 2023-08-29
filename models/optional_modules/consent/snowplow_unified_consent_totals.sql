{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    materialized='table',
    enabled=var("snowplow__enable_consent", false),
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with totals as (

  select
    last_consent_version,
    count(distinct domain_userid) as total_visitors,
    count(case when last_consent_event_type ='allow_all' then 1 end) as allow_all,
    count(case when last_consent_event_type ='allow_selected' then 1 end) as allow_selected,
    count(case when last_consent_event_type IN ('allow_all', 'allow_selected') then 1 end) as allow,
    count(case when last_consent_event_type = 'pending' then 1 end) as pending,
    count(case when last_consent_event_type = 'deny_all'  then 1 end) as denied,
    count(case when last_consent_event_type = 'expired'  then 1 end) as expired,
    count(case when last_consent_event_type = 'withdrawn'  then 1 end) as withdrawn,
    count(case when last_consent_event_type = 'implicit_consent'  then 1 end) as implicit_consent,
    count(case when {{ dateadd('year', '1', 'last_consent_event_tstamp') }} <= {{ dateadd('month', '6', 'current_date') }}
          and last_consent_event_type <> 'expired'
          and {{ dateadd('year', '1', 'last_consent_event_tstamp') }} > current_date then 1 end) as expires_in_six_months

  from {{ ref('snowplow_unified_consent_users') }}

  where last_consent_event_type is not null

  group by 1

)

select
  v.*,
  t.total_visitors,
  t.allow_all,
  t.allow_selected,
  t.allow,
  t.pending,
  t.denied,
  t.expired,
  t.withdrawn,
  t.implicit_consent,
  t.expires_in_six_months

from {{ ref('snowplow_unified_consent_versions') }} v

left join totals t
on t.last_consent_version = v.consent_version

order by v.version_start_tstamp desc
