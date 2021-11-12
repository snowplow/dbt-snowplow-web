{% docs table_base_sessions_lifecycle_manifest %}

This incremental table is a manifest of all sessions that have been processed by the Snowplow dbt web model. For each session, the start and end timestamp is recorded. 

By knowing the lifecycle of a session the model is able to able to determine which sessions and thus events to process for a given timeframe, as well as the complete date range required to reprocess all events of each session.

{% enddocs %}

{% docs table_base_incremental_manifest %}

This incremental table is a manifest of the timestamp of the latest event consumed per model within the `snowplow-web` package as well as any models leveraging the incremental framework provided by the package. The latest event's timestamp is based off `collector_tstamp`. This table is used to determine what events should be processed in the next run of the model.
{% enddocs %}

{% docs table_base_new_event_limits %}

This table contains the lower and upper timestamp limits for the given run of the web model. These limits are used to select new events from the events table.

{% enddocs %}


{% docs table_base_events_this_run %}

For any given run, this table contains all required events to be consumed by subsequent nodes in the Snowplow dbt web package. This is a cleaned, deduped dataset, containing all columns from the raw events table as well as having the `page_view_id` joined in from the page view context. 

**Note: This table should be used as the input to any custom modules that require event level data, rather than selecting straight from `atomic.events`**

{% enddocs %}


{% docs table_base_sessions_this_run %}

For any given run, this table contains all the required sessions.

{% enddocs %}


{% docs table_base_quarantined_sessions %}

This table contains any sessions that have been quarantined. Sessions are quarantined once they exceed the maximum allowed session length, defined by `snowplow__max_session_days`.
Once quarantined, no further events from these sessions will be processed. Events up until the point of quarantine remain in your derived tables.
The reason for removing long sessions is to reduce table scans on both the events table and all derived tables. This improves performance greatly.

{% enddocs %}
