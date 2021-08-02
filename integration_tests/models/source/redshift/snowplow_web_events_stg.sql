
select
  *

from {{ ref('snowplow_web_events') }}
where {{ edge_cases_to_ignore() }} --filter out any edge cases we havent yet solved for but are included in the test dataset.

