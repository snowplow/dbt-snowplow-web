{% docs table_base_sessions_lifecycle %}

This incremental table is a manifest of all sessions that have been processed by the Snowplow dbt web model. For each session, the start and end timestamp is recorded. 

By knowing the lifecycle of a session the model is able to able to determine which sessions and thus events to process for a given timeframe, as well as the complete date range required to reprocess all events of each session.

{% enddocs %}


{% docs table_current_incremental_tstamp %}

This table contains the lower and upper timestamp limits for the given run of the web model. These limits are used to select new events from the events table. 

It is updated at the start of each run of the model by the `snowplow_incremental_pre_hook()` macro, which runs as an `on-run-start` hook. Please refer to the documentation for details on how this macro determines the run limits.

{% enddocs %}


{% docs table_base_events_this_run %}

For any given run, this table contains all required events to be consumed by subsequent nodes in the Snowplow dbt web package. This is a cleaned, deduped dataset, containing all columns from the raw events table as well as having the `page_view_id` joined in from the page view context. 

**Note: This table should be used as the input to any custom modules that require event level data, rather than selecting straight from `atomic.events`**

{% enddocs %}
