{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select
  a.*,
  min(a.start_tstamp) over(partition by a.domain_userid) as user_start_tstamp,
  max(a.end_tstamp) over(partition by a.domain_userid) as user_end_tstamp

from {{ var('snowplow__sessions_table') }} a
where exists (select 1 from {{ ref('snowplow_web_base_sessions_this_run') }} b where a.domain_userid = b.user_identifier)
