
select
  *

from {{ ref('snowplow_web_events') }}

