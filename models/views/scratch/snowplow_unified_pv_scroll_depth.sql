{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (
  select
    ev.page_view_id,
    {% if var('snowplow__limit_page_views_to_session', true) %}
    ev.domain_sessionid,
    {% endif %}

    max(ev.doc_width) as doc_width,
    max(ev.doc_height) as doc_height,

    max(ev.br_viewwidth) as br_viewwidth,
    max(ev.br_viewheight) as br_viewheight,

    -- coalesce replaces null with 0 (because the page view event does send an offset)
    -- greatest prevents outliers (negative offsets)
    -- least also prevents outliers (offsets greater than the docwidth or docheight)

    least(greatest(min(coalesce(ev.pp_xoffset_min, 0)), 0), max(ev.doc_width)) as hmin, -- should be zero
    least(greatest(max(coalesce(ev.pp_xoffset_max, 0)), 0), max(ev.doc_width)) as hmax,

    least(greatest(min(coalesce(ev.pp_yoffset_min, 0)), 0), max(ev.doc_height)) as vmin, -- should be zero (edge case: not zero because the pv event is missing)
    least(greatest(max(coalesce(ev.pp_yoffset_max, 0)), 0), max(ev.doc_height)) as vmax

  from {{ ref('snowplow_unified_base_events_this_run') }} as ev

  where ev.event_name in ('page_view', 'page_ping')
    and ev.page_view_id is not null
    and ev.doc_height > 0 -- exclude problematic (but rare) edge case
    and ev.doc_width > 0 -- exclude problematic (but rare) edge case

  group by 1 {% if var('snowplow__limit_page_views_to_session', true) %}, 2 {% endif %}
)

select
  page_view_id,
  {% if var('snowplow__limit_page_views_to_session', true) %}
  domain_sessionid,
  {% endif %}

  doc_width,
  doc_height,

  br_viewwidth,
  br_viewheight,

  hmin,
  hmax,
  vmin,
  vmax,

  cast(round(100*(greatest(hmin, 0)/cast(doc_width as {{ type_float() }}))) as {{ type_float() }}) as relative_hmin, -- brackets matter: because hmin is of type int, we need to divide before we multiply by 100 or we risk an overflow
  cast(round(100*(least(hmax + br_viewwidth, doc_width)/cast(doc_width as {{ type_float() }}))) as {{ type_float() }}) as relative_hmax,
  cast(round(100*(greatest(vmin, 0)/cast(doc_height as {{ type_float() }}))) as {{ type_float() }}) as relative_vmin,
  cast(round(100*(least(vmax + br_viewheight, doc_height)/cast(doc_height as {{ type_float() }}))) as {{ type_float() }}) as relative_vmax -- not zero when a user hasn't scrolled because it includes the non-zero viewheight

from prep
