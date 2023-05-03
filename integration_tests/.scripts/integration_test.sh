#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "databricks" "postgres" "redshift" "snowflake")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "Snowplow web integration tests: Seeding data"

  eval "dbt seed --target $db --full-refresh" || exit 1;

  echo "Snowplow web integration tests: Execute models - run 1/4 (no contexts, no conversions)"

  eval "dbt run --target $db --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 243, snowplow__enable_iab: false, snowplow__enable_ua: false, snowplow__enable_yauaa: false, snowplow__conversion_events: , snowplow__total_all_conversions: false, snowplow__list_event_counts: false }'" || exit 1;

  echo "Snowplow web integration tests: Execute models - run 1/4"

  eval "dbt run --target $db --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 243}'" || exit 1;

  for i in {2..4}
  do
    echo "Snowplow web integration tests: Execute models - run $i/4"

    eval "dbt run --target $db" || exit 1;
  done

  echo "Snowplow web integration tests: Test models"

  eval "dbt test --target $db --store-failures" || exit 1;

  echo "Snowplow web integration tests: All tests passed"

done
