{# CWV tests run on a different source dataset, this is an easy way to hack them together. #}
{% if not var("snowplow__enable_cwv", false) %}

select
  *

from {{ ref('snowplow_web_events') }}

{% else %}

select
 *

from {{ ref('snowplow_web_vital_events') }}


{% endif %}
