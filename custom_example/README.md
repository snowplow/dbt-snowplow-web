# Snowplow Custom Example

This is a dummy dbt project demonstrating how to integrate custom modules into the Snowplow Web dbt package.

We demonstrate two methods to add custom modules, but more details are provided in our [docs](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-custom-models/).

## Set Up

- Install the Snowplow Web dbt package by [adding the package](https://docs.getdbt.com/docs/building-a-dbt-project/package-management) to the your package.yml file. See [dbt's package hub](https://hub.getdbt.com/snowplow/snowplow_web/latest/) for the latest instruction.
- Create a sub directory under `/models` to contain all your custom modules. We recommend `snowplow_web_custom_modules`.
- Add the tag `snowplow_unified_incremental` to your custom modules directory. This ensures all models in this directory are included in the incremental logic of the Snowplow Web package.

```yml
# dbt_project.yml
models:
  snowplow_custom_example:
    snowplow_web_custom_modules:
      +tags: snowplow_unified_incremental #Adds tag to all models in the 'snowplow_web_custom_modules' directory
```

- Redshift only: Add sources for any context or unstructured events tables that you need for your custom module:

```yml
# snowplow_sources.yml
sources:
  - name: atomic
    schema: "{{ var('snowplow__atomic_schema', 'atomic') }}"
    database: "{{ var('snowplow__database', target.database) }}"
    tables:
      - name: com_snowplowanalytics_snowplow_link_click_1
```

## Method 1 - Create standalone incremental custom module

This method takes your custom SQL and produces an incremental 'derived' custom table. This can then be joined back onto the standard derived tables produced by the Snowplow Web dbt package (page views, sessions, users). This methodology lends itself well to niche metrics or fields that are used infrequently during analysis. By having stand alone modules, you avoid bloating your derived tables with rarely used columns.

Another advantage of this method is if you want to change the logic in your custom module and replay all events through the new version, you don't have to also tear down the 'standard' table with corresponding level of aggregation  as the two are independent.

An example of such a set up for Redshift can be seen in [snowplow_web_pv_channel_engagement.sql](models/snowplow_web_custom_modules/page_views/page_view_channel_engagement/default/snowplow_web_pv_channel_engagement.sql). This model calculates page view engagement by channel by using metrics derived from link click events. Similar examples for BigQuery, Snowflake and Databricks can be found under [page_view_channel_engagement](models/snowplow_web_custom_modules/page_views/page_view_channel_engagement).

### Points to note

- We select events from `snowplow_web_base_events_this_run` rather than `atomic.events`. This ensures we only have the events required for this run, as well as not having to worry about de-duping events.
- We restrict the date range of the `com_snowplowanalytics_snowplow_link_click_1` source table using `snowplow_web_base_new_event_limits`, improving query performance. This is only required in Redshift due to the federated table design.
- We include the `is_run_with_new_events()` macro in the where clause. This ensures that no old data is inserted into the table during back-fills. This improves performance and protects against inaccurate data in the table during batched back-fills.
- The model is materialized using the `incremental` materialization with the `snowplow_optimize` config. This reduces the table scan on the target table during the upsert procedure.
- This incremental table can then be joined back to the `snowplow_web_page_views` table to produce a bespoke page views view catered for your business needs, `snowplow_page_views_custom`. Notice how this is materialized as a view, saving on storage cost.

## Method 2 - Replace a standard derived table with your own custom version

This method takes your custom SQL and produces as drop and recompute staging table, which can be joined into the relevant 'standard' Snowplow Web `_this_run`  table to produce a custom derived table. The key difference here is that you are replacing the default derived table such as `snowplow_web_sessions` with your own e.g. `snowplow_web_sessions_custom`. This methodology lends itself well to frequently used metrics or fields, which you would like readily available in your derived tables.

Note that if your logic in your custom module changes and you need to tear it down and back fill, you will also have to teardown the custom derived table e.g. `snowplow_web_sessions_custom`. As a result, you will have to replay events through both the standard sessions module and through your custom module to back fill your custom derived table.

An example of such a set up can be seen in [snowplow_web_sessions_conversion_this_run.sql](models/snowplow_web_custom_modules/sessions/sessions_conversion/snowplow_web_sessions_conversion_this_run.sql). This model calculates whether a conversion event occurred in a given session.

### Points to note

- We select from `snowplow_web_page_views_this_run` rather than `snowplow_web_page_views`. This ensures we only have the page views required for this run, as well as not having to worry about de-duping page views.
- It is materialized as a table rather than incremental. This is because this is a staging table, which will be joined into an incremental derived table. As such, we name the file with the `_this_run` suffix to reflect this.
- In `snowplow_web_sessions_custom`, we join together `snowplow_web_sessions_this_run` and `snowplow_web_sessions_conversion_this_run` to produce a bespoke incremental sessions table.
- We include the `is_run_with_new_events()` macro in the where clause. This ensures that no old data is inserted into the table during back-fills. This improves performance and protects against inaccurate data in the table during batched back-fills.
- We disable the default sessions table in our dbt_project.yml file so as not to duplicate datasets:

```yml
# dbt_project.yml
models:
  ...
  snowplow_web: # Only applies to models provided by the Snowplow Web dbt package
    sessions:
      snowplow_web_sessions:
        +enabled: false # Disable the snowplow_web_sessions model as we have our custom version, snowplow_web_sessions_custom
```

- We redirect references to the sessions table to our new, custom sessions table. **Note this is only required if you are disabling the sessions table since it is the only derived table referenced by the Snowplow Web package**

```yml
# dbt_project.yml
vars:
  snowplow_web:
    snowplow__sessions_table: "{{ ref('snowplow_web_sessions_custom') }}" # Redirect references to sessions table to your custom version.
```
