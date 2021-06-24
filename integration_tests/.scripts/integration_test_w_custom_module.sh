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

eval "dbt seed --target $DATABASE --full-refresh" || exit 1;

echo "Snowplow web integration tests: Run 1: standard modules"

eval "dbt run --target $DATABASE --full-refresh --vars 'teardown_all: true'" || exit 1;

echo "Snowplow web integration tests: Run 2: standard modules"

eval "dbt run --target $DATABASE" || exit 1;

echo "Snowplow web integration tests: Run 3: Partial backfill of custom module + standard modules"
# This tests the functionality of the snowplow_utils.is_run_with_new_events() macro
# Could be a scenario when a new custom module is added where:
# - the main scheduled snowplow job runs i.e. all modules + custom backfill
# - then the user manually runs a job in dbt cloud to just backfill new custom module.
# This results in the derived tables being partially backfilled

eval "dbt run --target $DATABASE --vars 'snowplow__enable_custom_example: true'" || exit 1;

echo "Snowplow web integration tests: Run 4: Partial backfill of custom module only"

eval "dbt run --models +snowplow_web_pv_channels --target $DATABASE --vars 'snowplow__enable_custom_example: true'" || exit 1;

for i in {5..6}
do
  echo "Snowplow web integration tests: Run $i/6: Standard increment - all modules"

  eval "dbt run --target $DATABASE --vars 'snowplow__enable_custom_example: true'" || exit 1;
done

echo "Snowplow web integration tests: Test models"

eval "dbt test --target $DATABASE" || exit 1;

echo "Snowplow web integration tests: All tests passed"
