
select *

from {{ ref('snowplow_web_base_quarantined_sessions') }}
