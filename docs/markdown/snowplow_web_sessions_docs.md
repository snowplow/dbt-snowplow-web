{% docs table_sessions_this_run %}

This staging table contains all the sessions for the given run of the Web model. It possess all the same columns as `snowplow_web_sessions`. If building a custom module that requires session level data, this is the table you should reference.

{% enddocs %}


{% docs table_sessions %}

This derived incremental table contains all historic sessions and should be the end point for any analysis or BI tools.

{% enddocs %}


{% docs table_sessions_aggs %}

This model aggregates various metrics derived from page views to a session level.

{% enddocs %}


{% docs table_sessions_lasts %}

This model identifies the last page view within a given session and returns various dimensions associated with that page view.

{% enddocs %}
