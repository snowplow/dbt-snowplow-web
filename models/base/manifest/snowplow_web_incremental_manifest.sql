{{ 
  config(
    materialized='incremental',
    full_refresh=snowplow_web.allow_refresh()
  ) 
}}

-- Boilerplate to generate table.
-- Table updated as part of end-run hook

{# Redshift produces varchar(1) column. Fixing char limit #}
{% set type_string = dbt_utils.type_string() %}
{% set type_string = 'varchar(4096)' if type_string == 'varchar' else type_string %}

with prep as (
  select
    cast(null as {{ type_string }}) model,
    cast('1970-01-01' as {{ dbt_utils.type_timestamp() }}) as last_success
)

select *

from prep
where false
