{{ 
  config(
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["page_view_id"]),
    sort='page_view_id',
    dist='page_view_id'
  ) 
}}

with prep as (
  select
    ev.page_view_id,

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

  from {{ ref('snowplow_web_base_events_this_run') }} as ev

  where ev.event_name in ('page_view', 'page_ping')
    and ev.page_view_id is not null
    and ev.doc_height > 0 -- exclude problematic (but rare) edge case
    and ev.doc_width > 0 -- exclude problematic (but rare) edge case

  group by 1
)

select
  page_view_id,

  doc_width,
  doc_height,

  br_viewwidth,
  br_viewheight,

  hmin,
  hmax,
  vmin,
  vmax,

  cast(round(100*(greatest(hmin, 0)/cast(doc_width as {{ dbt_utils.type_float() }}))) as {{ dbt_utils.type_float() }}) as relative_hmin, -- brackets matter: because hmin is of type int, we need to divide before we multiply by 100 or we risk an overflow
  cast(round(100*(least(hmax + br_viewwidth, doc_width)/cast(doc_width as {{ dbt_utils.type_float() }}))) as {{ dbt_utils.type_float() }}) as relative_hmax,
  cast(round(100*(greatest(vmin, 0)/cast(doc_height as {{ dbt_utils.type_float() }}))) as {{ dbt_utils.type_float() }}) as relative_vmin,
  cast(round(100*(least(vmax + br_viewheight, doc_height)/cast(doc_height as {{ dbt_utils.type_float() }}))) as {{ dbt_utils.type_float() }}) as relative_vmax -- not zero when a user hasn't scrolled because it includes the non-zero viewheight

from prep
