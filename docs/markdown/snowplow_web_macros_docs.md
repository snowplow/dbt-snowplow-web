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

#### Returns

The update/merge statement to update the `stitched_user_id` column, if enabled.
{% endraw %}
{% enddocs %}

{% docs macro_get_iab_context_fields %}
{% raw %}
This macro is used to extract the fields from the iab enrichment context for each warehouse.

#### Returns

The sql to extract the columns from the iab context, or these columns as nulls.
{% endraw %}
{% enddocs %}

{% docs macro_get_ua_context_fields %}
{% raw %}
This macro is used to extract the fields from the ua enrichment context for each warehouse.

#### Returns

The sql to extract the columns from the ua context, or these columns as nulls.
{% endraw %}
{% enddocs %}

{% docs macro_get_yauaa_context_fields %}
{% raw %}
This macro is used to extract the fields from the yauaa enrichment context for each warehouse.

#### Returns

The sql to extract the columns from the yauaa context, or these columns as nulls.
{% endraw %}
{% enddocs %}

{% docs macro_web_cluster_by_X %}
{% raw %}
This macro is used to return the appropriate `cluster_by` fields for the table, depending on the warehouse target.

#### Returns

The specific fields for each warehouse (see macro code for values).
{% endraw %}
{% enddocs %}

{% docs macro_bq_context_fields %}
{% raw %}
This macro is used to return the appropriate field and type mapping for use in `snowplow_utils.get_optional_fields`.

#### Returns

The specific fields and their type for the context (see macro code for values).
{% endraw %}
{% enddocs %}

{% docs macro_allow_refresh %}
{% raw %}
This macro is used to determine if a full-refresh is allowed (depending on the environment), using the `snowplow__allow_refresh` variable.

#### Returns
`snowplow__allow_refresh` if environment is not `dev`, `none` otherwise.

{% endraw %}
{% enddocs %}

{% docs macro_channel_group_query %}
{% raw %}
This macro returns the sql to identify the marketing channel from a url based on the `mkt_source`, `mkt_medium`, and `mkt_campaign` fields. It can be overwritten to use a different logic.

#### Returns
The sql to provide the classification (expected in the form of case when statements).

{% endraw %}
{% enddocs %}

{% docs macro_engaged_session %}
{% raw %}
This macro returns the sql to identify if a session is classed as engaged or not. It can be overwritten to use a different logic. By default any session that has 2 or more page views, more than 2 heartbeats worth of engaged time, or has any conversion events is classed as engaged.

Note that if you are overwriting this macro you have may not have immediate access to all fields in the derived sessions table, and may have to use a table alias to specify the column you wish to use, please see the definition of `snowplow_web_sessions_this_run` to identify which fields are available at the time of the macro call.

#### Returns
The sql defining an engaged session (true/false).

{% endraw %}
{% enddocs %}

{% docs macro_core_web_vital_results_query %}
{% raw %}
This macro is used to let the user classify the tresholds to be applied for the measurements. Please make sure you set the results you would like the measurements to pass to **`good`** or align it with the `macro_core_web_vital_pass_query` macro.

#### Returns
The sql to provide the logic for the evaluation based on user defined tresholds (expected in the form of case when statements).

{% endraw %}
{% enddocs %}

{% docs macro_core_web_vital_page_groups %}
{% raw %}
This macro is used to let the user classify page urls into page groups.

#### Returns
The sql to provide the classification (expected in the form of case when statements).

{% endraw %}
{% enddocs %}

{% docs macro_content_group_query %}
{% raw %}
This macro is used to let the user classify page urls into content groups.

#### Returns
The sql to provide the classification (expected in the form of case when statements).

{% endraw %}
{% enddocs %}

{% docs macro_core_web_vital_pass_query %}
{% raw %}
This macro is used to let the user define what counts as the overall pass condition for the core web vital measurements.

#### Returns
The sql to provide the logic for the evaluation based on user defined tresholds (expected in the form of case when statements).

{% endraw %}
{% enddocs %}


