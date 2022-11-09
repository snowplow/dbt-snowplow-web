{{
  config(
    materialized='table',
  )
}}

with arrays as (

  select
    domain_userid,
    split(last_consent_scopes, ', ') as scope_array

  from {{ ref('snowplow_web_consent_users') }}

  where is_latest_version

  )

  , unnesting as (

    {{ snowplow_utils.unnest('domain_userid', 'scope_array', 'consent_scope', 'arrays') }}

  )

select
  replace(consent_scope,'"', '') as scope,
  count(*) as total_consent

from unnesting

group by 1
