{# Filters out edge cases in test data set which we haven't yet solved for #}
{% macro edge_cases_to_ignore() %}
  user_id not in (
    'stray page ping', -- Known unsolved issue https://github.com/snowplow/data-models/issues/92
    'NULL domain_userid' -- Case when `domain_userid` is null but `domain_sessionid` is not null. Shouldn't happen. Will solve if it arises.
    )
{% endmacro %}
