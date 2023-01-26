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
