with prep as (

  select
    consent_version,
    count(*) as n_consents

  from {{ ref('snowplow_web_consent_versions')}}

  group by 1

  having count(*)>1
)

select * from prep
