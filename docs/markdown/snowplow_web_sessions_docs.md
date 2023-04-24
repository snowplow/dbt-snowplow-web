{% docs table_sessions_this_run %}

This staging table contains all the sessions for the given run of the Web model. It possess all the same columns as `snowplow_web_sessions`. If building a custom module that requires session level data, this is the table you should reference.

{% enddocs %}


{% docs table_sessions %}

This derived incremental table contains all historic sessions and should be the end point for any analysis or BI tools.

{% enddocs %}
