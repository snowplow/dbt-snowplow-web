{% macro filter_bots(table_alias = none) %}
  {{ return(adapter.dispatch('filter_bots', 'snowplow_web')(table_alias)) }}
{%- endmacro -%}

{% macro default__filter_bots(table_alias = none) %}
  and {% if table_alias %}{{table_alias~'.'}}{% endif %}useragent not similar to '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
{% endmacro %}

{% macro bigquery__filter_bots(table_alias = none) %}
  and not regexp_contains({% if table_alias %}{{table_alias~'.'}}{% endif %}useragent, '(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)')
{% endmacro %}

{% macro databricks__filter_bots(table_alias = none) %}
  and not rlike({% if table_alias %}{{table_alias~'.'}}{% endif %}useragent, '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*')
{% endmacro %}

{% macro snowflake__filter_bots(table_alias = none) %}
  and not rlike({% if table_alias %}{{table_alias~'.'}}{% endif %}useragent, '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*')
{% endmacro %}

{% macro redshift__filter_bots(table_alias = none) %}
  and {% if table_alias %}{{table_alias~'.'}}{% endif %}useragent not similar to '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
{% endmacro %}
