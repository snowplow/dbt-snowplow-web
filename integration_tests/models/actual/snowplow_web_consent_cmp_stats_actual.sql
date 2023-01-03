select *

from {{ ref('snowplow_web_consent_cmp_stats') }}
