{{ 
  config(
    materialized='incremental',
    full_refresh=snowplow_web.allow_refresh()
  ) 
}}

-- Boilerplate to generate table.
-- Table updated as part of end-run hook

with prep as (
  select
    cast(null as {{ snowplow_utils.type_string(4096) }}) model,
    cast('1970-01-01' as {{ dbt_utils.type_timestamp() }}) as last_success
)

select *

from prep
where false
