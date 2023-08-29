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

{%- if target.type in ('postgres') -%}

with events as (

  select
    event_id,
    domain_userid,
    original_domain_userid,
    page_view_id,
    domain_sessionid,
    original_domain_sessionid,
    derived_tstamp,
    event_name,
    event_type,
    cmp_load_time,
    -- postgres does not allow the IGNORE NULL clause within last_value(), below workaround should do the same: removing NULLS using array_remove then using the COUNT window function (which counts the number of non-null items and count is bounded up to the current row) to access the array using that as its index position
    (array_remove(array_agg(case when event_name = 'cmp_visible' then event_id else null end) over (partition by domain_userid order by derived_tstamp), null))[count(case when event_name = 'cmp_visible' then event_id else null end) over (partition by domain_userid order by derived_tstamp rows between unbounded preceding and current row)] as cmp_id

  from {{ ref('snowplow_unified_consent_log') }}

  where event_type <> 'pending' or event_type is null

)

{%- elif target.type in ('databricks', 'spark') -%}

with events as (

   select
    event_id,
    domain_userid,
    original_domain_userid,
    page_view_id,
    domain_sessionid,
    original_domain_sessionid,
    derived_tstamp,
    event_name,
    event_type,
    cmp_load_time,
    last_value(case when event_name = 'cmp_visible' then event_id else null end, TRUE)
    over (partition by domain_userid order by derived_tstamp
    rows between unbounded preceding and current row) as cmp_id

  from {{ ref('snowplow_unified_consent_log') }}

  where event_type <> 'pending' or event_type is null

)

{%- else -%}

with events as (

  select
    event_id,
    domain_userid,
    original_domain_userid,
    page_view_id,
    domain_sessionid,
    original_domain_sessionid,
    derived_tstamp,
    event_name,
    event_type,
    cmp_load_time,
    last_value(case when event_name = 'cmp_visible' then event_id else null end ignore nulls)
    over (partition by domain_userid order by derived_tstamp
    rows between unbounded preceding and current row) as cmp_id

  from {{ ref('snowplow_unified_consent_log') }}

  where event_type <> 'pending' or event_type is null

)

{%- endif -%}

, event_orders as (

  select
    event_id,
    event_type,
    cmp_id,
    derived_tstamp,
    row_number() over(partition by cmp_id order by derived_tstamp) as row_num

  from events

)

, first_consent_events as (

  select
    event_id,
    cmp_id,
    event_type,
    derived_tstamp as first_consent_event_tstamp

  from event_orders

  where row_num = 2

)

, cmp_events as (

  select distinct
    event_id,
    domain_userid,
    original_domain_userid,
    page_view_id,
    domain_sessionid,
    original_domain_sessionid,
    cmp_load_time,
    derived_tstamp as cmp_tstamp

  from events

  where event_name = 'cmp_visible'

)

select
  e.event_id,
  e.domain_userid,
  e.original_domain_userid,
  e.page_view_id,
  e.domain_sessionid,
  e.original_domain_sessionid,
  e.cmp_load_time,
  e.cmp_tstamp,
  f.first_consent_event_tstamp,
  f.event_type as first_consent_event_type,
  {{ datediff('e.cmp_tstamp', 'f.first_consent_event_tstamp', 'second') }} as cmp_interaction_time

from cmp_events e

left join first_consent_events f
on e.event_id = f.cmp_id
