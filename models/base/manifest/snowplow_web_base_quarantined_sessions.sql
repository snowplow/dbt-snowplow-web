{{ 
  config(
    materialized='incremental',
    full_refresh=snowplow_web.allow_refresh()
  ) 
}}

/* 
Boilerplate to generate table.
Table updated as part of post-hook on sessions_this_run
Any sessions exceeding max_session_days are quarantined
Once quarantined, any subsequent events from the session will not be processed.
This significantly reduces table scans
*/

{# Redshift produces varchar(1) column. Fixing char limit #}
{% set type_string = dbt_utils.type_string() %}
{% set type_string = 'varchar(64)' if type_string == 'varchar' else type_string %}

with prep as (
  select
    cast(null as {{ type_string }}) session_id
)

select *

from prep
where false
