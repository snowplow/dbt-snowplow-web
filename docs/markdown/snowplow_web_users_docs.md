
{% docs table_users_this_run %}

This staging table contains all the users for the given run of the Web model. It possess all the same columns as `snowplow_web_users`. If building a custom module that requires session level data, this is the table you should reference.

{% enddocs %}


{% docs table_users %}

This derived incremental table contains all historic users data and should be the end point for any analysis or BI tools.

{% enddocs %}


{% docs table_users_aggs %}

This model aggregates various metrics derived from sessions to a users level.

{% enddocs %}


{% docs table_users_lasts %}

This model identifies the last page view for a user and returns various dimensions associated with that page view.

{% enddocs %}


{% docs table_users_sessions_this_run %}

This model contains all sessions data related to users contained in the given run of the Web model 

{% enddocs %}
