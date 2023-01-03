select *

from {{ ref('snowplow_web_consent_totals') }}
