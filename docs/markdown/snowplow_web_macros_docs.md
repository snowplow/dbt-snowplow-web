{% docs macro_filter_bots %}
{% raw %}		
This macro is used to generate a warehouse specific filter for the `useragent` field to remove bots from processing, or to overwrite for custom filtering. The filter excludes any of the following in the string:
- bot
- crawl
- slurp
- spider
- archiv
- spinn
- sniff
- seo
- audit
- survey
- pingdom
- worm
- capture
- (browser|screen)shots
- analyz
- index
- thumb
- check
- facebook
- PingdomBot
- PhantomJS
- YandexBot
- Twitterbot
- a_archiver
- facebookexternalhit
- Bingbot
- BingPreview
- Googlebot
- Baiduspider
- 360(Spider|User-agent)
- semalt

#### Returns

A filter on `useragent` to exclude those with strings matching the above list.

#### Usage

```sql
select
...
from
...
where 1=1
filter_bots()

-- returns (snowflake)
select
...
from
...
where 1=1
and not rlike(useragent, '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*')
```
{% endraw %}
{% enddocs %}

{% docs macro_stitch_user_identifiers %}
{% raw %}		
This macro is used as a post-hook on the sessions table to stitch user identities using the user_mapping table provided.
- 
#### Returns

The update/merge statement to update the `stitched_user_id` column, if enabled.
{% endraw %}
{% enddocs %}

{% docs macro_get_iab_context_fields %}
{% raw %}		
This macro is used to extract the fields from the iab enrichment context for each warehouse.
- 
#### Returns

The sql to extract the columns from the iab context, or these columns as nulls.
{% endraw %}
{% enddocs %}

{% docs macro_get_ua_context_fields %}
{% raw %}		
This macro is used to extract the fields from the ua enrichment context for each warehouse.
- 
#### Returns

The sql to extract the columns from the ua context, or these columns as nulls.
{% endraw %}
{% enddocs %}

{% docs macro_get_yauaa_context_fields %}
{% raw %}		
This macro is used to extract the fields from the yauaa enrichment context for each warehouse.
- 
#### Returns

The sql to extract the columns from the yauaa context, or these columns as nulls.
{% endraw %}
{% enddocs %}

{% docs macro_web_cluster_by_X %}
{% raw %}		
This macro is used to return the appropriate `cluster_by` fields for the table, depending on the warehouse target.
- 
#### Returns

The specific fields for each warehouse (see macro code for values).
{% endraw %}
{% enddocs %}

{% docs macro_bq_context_fields %}
{% raw %}		
This macro is used to return the appropriate field and type mapping for use in `snowplow_utils.get_optional_fields`.
- 
#### Returns

The specific fields and their type for the context (see macro code for values).
{% endraw %}
{% enddocs %}

{% docs macro_allow_refresh %}
{% raw %}		
This macro is used to determine if a full-refresh is allowed (depending on the environment), using the `snowplow__allow_refresh` variable.
- 
#### Returns
`snowplow__allow_refresh` if environment is not `dev`, `none` otherwise.

{% endraw %}
{% enddocs %}
