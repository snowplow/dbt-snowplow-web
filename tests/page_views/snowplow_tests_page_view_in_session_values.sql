
with prep as (
  select 
    domain_sessionid, 
    count(distinct page_views_in_session) as dist_pvis_values,
    count(*) - count(distinct page_view_in_session_index)  as all_minus_dist_pvisi,
    count(*) - count(distinct page_view_id) as all_minus_dist_pvids 

  from {{ ref('snowplow_web_page_views') }}
  group by 1
)

select
  domain_sessionid

from prep

where dist_pvis_values != 1
or all_minus_dist_pvisi != 0
or all_minus_dist_pvids != 0
