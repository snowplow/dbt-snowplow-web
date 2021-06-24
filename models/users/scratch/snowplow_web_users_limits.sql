select
  min(start_tstamp) as lower_limit,
  max(start_tstamp) as upper_limit

from {{ ref('snowplow_web_users_userids_this_run') }}

