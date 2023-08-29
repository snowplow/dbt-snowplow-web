{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}


select
  root_id,
  root_tstamp::timestamp as root_tstamp,
  basis_for_processing,
  consent_version,
  consent_scopes,
  domains_applied,
  consent_url,
  event_type,
  gdpr_applies::boolean as gdpr_applies,
  'consent_preferences' as schema_name

from {{ ref('snowplow_unified_consent_preferences') }}
