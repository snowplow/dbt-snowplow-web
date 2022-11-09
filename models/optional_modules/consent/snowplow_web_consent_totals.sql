{{
  config(
    materialized='table',
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
    count(case when {{ dbt_utils.dateadd('year', '1', 'last_consent_event_tstamp') }} <= {{ dbt_utils.dateadd('month', '6', 'current_date') }}
          and last_consent_event_type <> 'expired'
          and {{ dbt_utils.dateadd('year', '1', 'last_consent_event_tstamp') }} > current_date then 1 end) as expires_in_six_months

  from {{ ref('snowplow_web_consent_users') }}

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

from {{ ref('snowplow_web_consent_versions') }} v

left join totals t
on t.last_consent_version = v.consent_version

order by v.version_start_tstamp desc

