{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

with prep as (

  select
    consent_version,
    count(*) as n_consents

  from {{ ref('snowplow_unified_consent_versions')}}

  group by 1

  having count(*)>1
)

select * from prep
