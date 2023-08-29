{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{# Test out the overwrite works by taking a false to a true #}

{% macro default__content_group_query() %}
  case when ev.page_view_id = 'ff8cc048-afe8-4913-843d-37de6b7d87d0' then 'Look no further, I am the test subject!'
      when ev.page_url like '%/product%' then 'PDP'
      when ev.page_url like '%/list%' then 'PLP'
      when ev.page_url like '%/checkout%' then 'checkout'
      when ev.page_url like '%/home%' then 'homepage'
      else 'other'
  end
{% endmacro %}
