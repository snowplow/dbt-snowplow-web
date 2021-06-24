-- materialized as a view since we are just joining two production tables.
{{ 
  config(
    materialized='view'
  ) 
}}


select 
  pv.*,
  ce.link_clicks,
  ce.first_link_target,
  ce.is_bounced_page_view,
  ce.engagement_score

from {{ ref('snowplow_web_page_views') }} pv -- Join together the two incremental production tables
left join {{ ref('snowplow_web_pv_channel_engagement')}} ce
on pv.page_view_id = ce.page_view_id
