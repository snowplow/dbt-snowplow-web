#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

echo "Snowplow web integration tests: Seeding data"

eval dbt seed --target $DATABASE --full-refresh

echo "Snowplow web integration tests: Execute models - run 1/4"

eval "dbt run --target $DATABASE --full-refresh --vars 'snowplow_web_teardown_all: true'"

eval dbt run --target $DATABASE

eval "dbt run --models +snowplow_web_pv_channel_engagement --target $DATABASE --full-refresh --vars 'snowplow__enable_custom_example: true'"

eval "dbt run --models +snowplow_web_pv_channel_engagement --target $DATABASE --vars 'snowplow__enable_custom_example: true'"

eval "dbt run --target $DATABASE --vars 'snowplow__enable_custom_example: true'"

eval "dbt run --target $DATABASE --vars 'snowplow__enable_custom_example: true'"

echo "Snowplow web integration tests: Test models"

eval dbt test --target $DATABASE

echo "Snowplow web integration tests: All tests passed"
