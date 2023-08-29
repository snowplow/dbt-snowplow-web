{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

select
  root_id,
  root_tstamp::timestamp,
  category,
  primaryImpact as primary_impact,
  reason,
  spiderOrRobot::boolean as spider_or_robot,
  schema_name

from {{ ref('snowplow_unified_spider_context') }}
