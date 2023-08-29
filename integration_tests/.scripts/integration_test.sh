#!/bin/bash

# Expected input:
# -d (database) target database for dbt

while getopts 'd:' opt
do
  case $opt in
    d) DATABASE=$OPTARG
  esac
done

declare -a SUPPORTED_DATABASES=("bigquery" "postgres" "databricks" "redshift" "snowflake")

# set to lower case
DATABASE="$(echo $DATABASE | tr '[:upper:]' '[:lower:]')"

if [[ $DATABASE == "all" ]]; then
  DATABASES=( "${SUPPORTED_DATABASES[@]}" )
else
  DATABASES=$DATABASE
fi

for db in ${DATABASES[@]}; do

  echo "Snowplow unified integration tests: Seeding data"

  eval "dbt seed --full-refresh --target $db" || exit 1;

  echo "Snowplow unified integration tests: Execute models (no contexts, no conversions)"

  eval "dbt run  --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 243, snowplow__enable_iab: false, snowplow__enable_ua: false, snowplow__enable_yauaa: false, snowplow__conversion_events: , snowplow__total_all_conversions: false, snowplow__list_event_counts: false, snowplow__enable_cwv: false, snowplow__enable_consent: false}' --target $db" || exit 1;

  echo "Snowplow unified integration tests: Execute models - run 1/4"

  eval "dbt run --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__backfill_limit_days: 243, snowplow__enable_cwv: false}' --target $db" || exit 1;

  for i in {2..4}
  do
    echo "Snowplow unified integration tests: Execute models - run $i/4"

    eval "dbt run --vars '{snowplow__enable_cwv: false}' --target $db" || exit 1;
  done

  echo "Snowplow unified integration tests: Test models"

  eval "dbt test --exclude snowplow_unified_web_vital_measurements snowplow_unified_web_vital_measurements_actual snowplow_unified_web_vital_events_this_run --store-failures --target $db" || exit 1;

  echo "Snowplow unified integration tests: All non-CWV tests passed"

  echo "Snowplow unified integration tests - Core Web Vitals: Execute models"

  eval "dbt run --select +snowplow_unified_web_vital_measurements_actual snowplow_unified_web_vital_measurements_expected_stg source  --full-refresh --vars '{snowplow__allow_refresh: true, snowplow__start_date: '2023-03-01', snowplow__backfill_limit_days: 50, snowplow__cwv_days_to_measure: 999}' --target $db" || exit 1;

  eval "dbt test --select snowplow_unified_web_vital_measurements_actual --store-failures --target $db" || exit 1;

  echo "Snowplow unified integration tests: All CWV tests passed"

done
