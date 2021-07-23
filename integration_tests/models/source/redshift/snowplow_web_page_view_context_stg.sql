
select
  *

from {{ ref('snowplow_web_page_view_context') }}

