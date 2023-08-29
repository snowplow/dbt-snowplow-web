{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}


select
  root_id,
  root_tstamp::timestamp,
  elapsed_time,
  'cmp_visible' as schema_name

from {{ ref('snowplow_web_consent_cmp_visible') }}
