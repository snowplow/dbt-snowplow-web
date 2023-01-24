{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with user_ids_this_run as (
  select
      distinct domain_userid

  from {{ ref('snowplow_web_base_sessions_this_run') }}
  where domain_userid is not null
)

select
  a.*,
  min(a.start_tstamp) over(partition by a.domain_userid) as user_start_tstamp,
  max(a.end_tstamp) over(partition by a.domain_userid) as user_end_tstamp

from {{ var('snowplow__sessions_table') }} a
inner join user_ids_this_run b
on a.domain_userid = b.domain_userid
