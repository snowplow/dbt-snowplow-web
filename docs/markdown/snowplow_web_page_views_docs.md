{% docs table_page_views_this_run %}

This staging table contains all the page views for the given run of the Web model. It possess all the same columns as `snowplow_web_page_views`. If building a custom module that requires page view events, this is the table you should reference.

{% enddocs %}


{% docs table_page_views %}

This derived incremental table contains all historic page views and should be the end point for any analysis or BI tools.

{% enddocs %}


{% docs table_pv_engaged_time %}

This model calculates the time a visitor spent engaged on a given page view. This is calculated using the number of page ping events received for that page view.

{% enddocs %}

{% docs table_scroll_depth %}

This model calculates the horizontal and vertical scroll depth of the visitor on a given page view. Such metrics are useful when assessing engagement on a page view.

{% enddocs %}
