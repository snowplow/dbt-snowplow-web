{#
Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro filter_bots(table_alias = none) %}
  {{ return(adapter.dispatch('filter_bots', 'snowplow_web')(table_alias)) }}
{%- endmacro -%}

{% macro default__filter_bots(table_alias = none) %}
  and lower({% if table_alias %}{{table_alias~'.'}}{% endif %}useragent) not similar to '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|pingdombot|phantomjs|yandexbot|twitterbot|a_archiver|facebookexternalhit|bingbot|bingpreview|googlebot|baiduspider|360(spider|user-agent)|semalt)%'
{% endmacro %}

{% macro bigquery__filter_bots(table_alias = none) %}
  and not regexp_contains(lower({% if table_alias %}{{table_alias~'.'}}{% endif %}useragent), '(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|pingdombot|phantomjs|yandexbot|twitterbot|a_archiver|facebookexternalhit|bingbot|bingpreview|googlebot|baiduspider|360(spider|user-agent)|semalt)')
{% endmacro %}

{% macro spark__filter_bots(table_alias = none) %}
  and not rlike(lower({% if table_alias %}{{table_alias~'.'}}{% endif %}useragent), '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|pingdombot|phantomjs|yandexbot|twitterbot|a_archiver|facebookexternalhit|bingbot|bingpreview|googlebot|baiduspider|360(spider|user-agent)|semalt).*')
{% endmacro %}

{% macro snowflake__filter_bots(table_alias = none) %}
  and not rlike(lower({% if table_alias %}{{table_alias~'.'}}{% endif %}useragent), '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|pingdombot|phantomjs|yandexbot|twitterbot|a_archiver|facebookexternalhit|bingbot|bingpreview|googlebot|baiduspider|360(spider|user-agent)|semalt).*')
{% endmacro %}
