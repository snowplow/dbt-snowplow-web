{% docs table_users_manifest %}

This manifest table contains the timestamp when the user was first observed, based on `derived_tstamp`. It is a manifest of all historical users. It is used to determine how far back to scan the `snowplow_web_sessions` table to find all sessions for that users.  

{% enddocs %}

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


{% docs table_users_limits %}

This model calculates the lower and upper limit for all users within a given run. It is used to determine how far back to scan the `snowplow_web_sessions` table to find all sessions for that users.  

{% enddocs %}


{% docs table_users_sessions_this_run %}

This model contains all sessions data related to users contained in the given run of the Web model 

{% enddocs %}

{% docs table_users_userids_this_run %}

This model contains the first time a user was observed, `start_date`, for all users in a given run of the Web model.

{% enddocs %}
